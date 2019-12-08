package main

import (
	//"bytes"
	"flag"
	"fmt"
	"github.com/jamesjj/podready"
	"github.com/jamiealquiza/envy"
	"math/rand"
	"os"
	"sync"
	"time"
)

var (
	wg sync.WaitGroup
)

type config struct {
	sqsName                  *string
	sqsRegion                *string
	s3Name                   *string
	s3Region                 *string
	sqsPollTimeout           *int64
	sqsPollMaxMessages       *int64
	retryTime                *int64
	doneAfterCountEmptyPolls *int
	maxRecordsPerFile        *int
	moveFilesAfterProcessing *string
	logVerbose               *bool
	sqsDelete                *bool
	runDate                  *string
}

type copyTaskItem struct {
	sqsReceipt   string
	sourceBucket string
	sourceRegion string
	destBucket   string
	destRegion   string
	sourcePath   string
	destPath     string
	localPath    string
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	runDate := time.Now().UTC().Format("20060102")

	conf := config{
		flag.String("sqs", "", "Name of the SQS queue to poll [MANDATORY]"),
		flag.String("sqsregion", "", "AWS region of SQS queue [MANDATORY]"),
		flag.String("bucket", "", "Name of the S3 bucket to store TSV files [MANDATORY]"),
		flag.String("bucketregion", "", "AWS region of S3 bucket [MANDATORY]"),
		flag.Int64("polltimeout", 10, "SQS slow poll timeout, 1-20"),
		flag.Int64("pollmessages", 10, "SQS maximum messages per poll, 1-10"),
		flag.Int64("retrydelay", 600, "Retry delay [SUGGEST: DO NOT CHANGE]"),
		flag.Int("emptypolls", 3, "How many consecutive times to poll SQS and receive zero messages before exiting, 1+"),
		flag.Int("maxrecords", 32, "Maximum number * 1024 of records in a single S3 file, 1+, e.g 2 sets the limit to 2048"),
		flag.String("move", "", "Move email to this S3 prefix after processing. Date will be automatically added"),
		flag.Bool("verbose", false, "Show detailed information during run"),
		flag.Bool("deletesqs", true, "Delete messages from SQS after processing"),
		&runDate,
	}
	envy.Parse("S3TOS3")
	flag.Parse()

	if *conf.sqsName == "" ||
		*conf.sqsRegion == "" ||
		*conf.s3Name == "" ||
		*conf.s3Region == "" ||
		*conf.sqsPollTimeout < 1 ||
		*conf.sqsPollTimeout > 20 ||
		*conf.sqsPollMaxMessages > 20 ||
		*conf.sqsPollMaxMessages < 1 ||
		*conf.maxRecordsPerFile < 1 ||
		*conf.doneAfterCountEmptyPolls < 1 {
		flag.Usage()
		os.Exit(1)
	}

	logInit(conf)
	gracefulStop(func() {})
	podready.Wait()

	// TODO: control parallelism in channel buffers here
	refreshMessageVisibilityChan := make(chan *copyTaskItem)
	downloadFromS3Chan := make(chan *copyTaskItem)
	uploadToS3Chan := make(chan *copyTaskItem)
	deleteSqsChan := make(chan *copyTaskItem, 128)

	sqsQ, sqsErr := sqsClient(*conf.sqsRegion, *conf.sqsName)
	if sqsErr != nil {
		panic(sqsErr)
	}

	// DELETE SQS WORKER
	wg.Add(1)
	go func(conf config, wg *sync.WaitGroup, deleteSqsChan chan *copyTaskItem) {
		defer Debug.Printf("Worker finished: Delete")
		defer wg.Done()

		for task := range deleteSqsChan {
			if err := deleteSqs(sqsQ, task.sqsReceipt); err != nil {
				Error.Printf("Delete SQS: %#v", err)
				time.Sleep(3 * time.Second)
			}
		}
	}(conf, &wg, deleteSqsChan)

	// REFRESH VISIBILITY SQS WORKER
	wg.Add(1)
	go func(conf config, wg *sync.WaitGroup, refreshMessageVisibilityChan chan *copyTaskItem) {
		defer Debug.Printf("Worker finished: SQS Refresh")
		defer wg.Done()

		var refreshList []*copyTaskItem

		for {

			select {
			case task, more := <-refreshMessageVisibilityChan:
				if !more {
					return
				}
				refreshList = append(refreshList, task)
			default:
			}

			for i := 0; i < len(refreshList); {
				if err := updateVisibilitySqs(sqsQ, refreshList[i].sqsReceipt, 180); err != nil {
					refreshList = append(refreshList[:i], refreshList[i+1:]...)
					continue
				}
				i++
			}
			if len(refreshList) > 0 {
				Debug.Printf("SQS in refresh list: %d\n", len(refreshList))
				time.Sleep(time.Duration(60/(1+len(refreshList))) * time.Second)
			}
		}
	}(conf, &wg, refreshMessageVisibilityChan)

	// UPLOAD TO S3 WORKERS
	wg.Add(1)
	go func(conf config, wg *sync.WaitGroup) {
		defer Debug.Printf("Worker finished: Upload")
		defer wg.Done()
		defer close(deleteSqsChan)

		for task := range uploadToS3Chan { // TODO: Fix filename
			S3Upload( // TODO: ERROR CHECKING
				&task.destBucket,
				&task.destPath,
				&task.destRegion,
				&task.localPath,
			)
			// if no error then:
			deleteSqsChan <- task

		}
	}(conf, &wg)

	// DOWNLOAD FROM S3 WORKERS
	wg.Add(1)
	go func(conf config, wg *sync.WaitGroup) {
		defer Debug.Printf("Worker finished: Download")
		defer wg.Done()
		defer close(uploadToS3Chan)

		for task := range downloadFromS3Chan {

			s3Retry, errS3Download := S3Download(
				&task.sourceBucket,
				&task.sourcePath,
				&task.sourceRegion,
				&task.localPath,
			)
			if errS3Download == nil {
				Debug.Printf("Downloaded OK: %v", task.sourcePath)
				uploadToS3Chan <- task

			} else {
				Error.Printf(
					"Failed to download from S3: s3://%s/%s (retry_later=%v)",
					task.sourceBucket,
					task.sourcePath,
					s3Retry,
				)
				if s3Retry == true {
					// TODO sqs pushback
				} else {
					deleteSqsChan <- task
				}

			}

		}
	}(conf, &wg)

	// POLL SQS LOOP
	pollCount := *conf.doneAfterCountEmptyPolls
	for pollCount > 0 {

		Debug.Printf("pollCount=%d", pollCount)

		s3records, err := PollSQS(conf)
		if err != nil {
			Error.Printf("Failed to poll SQS: %v", err)
			pollCount--
			continue
		}

		pollCount--
		for _, s3msgs := range s3records {
			pollCount = *conf.doneAfterCountEmptyPolls

			for _, msgRecord := range s3msgs.Records {

				localFileName := fmt.Sprintf(
					"%s/s3-to-s3-file-%s-%s.bin.tmp",
					"/tmp",
					time.Now().UTC().Format("20060102-150405"),
					RandStringBytes(16),
				)

				item := copyTaskItem{
					s3msgs.ReceiptHandle,
					msgRecord.S3.Bucket.Name,
					msgRecord.AwsRegion,
					*conf.s3Name,
					*conf.s3Region,
					msgRecord.S3.Object.Key,
					fmt.Sprintf("output/%s", msgRecord.S3.Object.Key),
					localFileName,
				}

				refreshMessageVisibilityChan <- &item
				downloadFromS3Chan <- &item
				Debug.Printf("copyTaskItem: %#v", item)
				/* if *conf.sqsDelete && !s3Retry {
					deleteSqsChan <- &s3msgs.ReceiptHandle
				} */

			}
		}
	}
	close(refreshMessageVisibilityChan)
	close(downloadFromS3Chan)
	wg.Wait()

}
