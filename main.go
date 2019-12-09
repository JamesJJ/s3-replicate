package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/jamesjj/podready"
	"github.com/jamiealquiza/envy"
	"math/rand"
	"os"
	"strings"
	"sync"
	"text/template"
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
	doneAfterCountEmptyPolls *int64
	parallelTransfers        *int64
	targetPath               *string
	logVerbose               *bool
	runDate                  *string
}

type pathStruct struct {
	OriginalPath string
	KeyParts     []string
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
		flag.Int64("polltimeout", 18, "SQS slow poll timeout, 1-20"),
		flag.Int64("pollmessages", 10, "SQS maximum messages per poll, 1-10"),
		flag.Int64("emptypolls", 3, "How many consecutive times to poll SQS and receive zero messages before exiting, 1+"),
		flag.Int64("parallel", 16, "How many files to transfer concurrently"),
		flag.String("targetpath", "{{ .OriginalPath }}", "Target path go-template"),
		flag.Bool("verbose", false, "Show detailed information during run"),
		&runDate,
	}
	envy.Parse("S3TOS3")
	flag.Parse()

	if *conf.sqsName == "" ||
		*conf.sqsRegion == "" ||
		*conf.s3Name == "" ||
		*conf.s3Region == "" ||
		*conf.targetPath == "" ||
		*conf.sqsPollTimeout < 1 ||
		*conf.sqsPollTimeout > 20 ||
		*conf.sqsPollMaxMessages > 20 ||
		*conf.sqsPollMaxMessages < 1 ||
		*conf.doneAfterCountEmptyPolls < 1 {
		flag.Usage()
		os.Exit(1)
	}

	refreshMessageVisibilityChan := make(chan *copyTaskItem, 128)
	stopRefreshMessageVisibilityChan := make(chan *copyTaskItem, 128)
	downloadFromS3Chan := make(chan *copyTaskItem, *conf.parallelTransfers)
	uploadToS3Chan := make(chan *copyTaskItem, *conf.parallelTransfers)
	deleteSqsChan := make(chan *copyTaskItem, 128)

	logInit(conf)
	gracefulStop(func() {
		*conf.doneAfterCountEmptyPolls = -1
		time.Sleep(9 * time.Second)
	})
	podready.Wait()

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
			} else {
				stopRefreshMessageVisibilityChan <- task
			}
		}
	}(conf, &wg, deleteSqsChan)

	// REFRESH VISIBILITY SQS WORKER
	wg.Add(1)
	go func(conf config, wg *sync.WaitGroup, refreshMessageVisibilityChan chan *copyTaskItem, stopRefreshMessageVisibilityChan chan *copyTaskItem) {
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

			select {
			case task, more := <-stopRefreshMessageVisibilityChan:
				if !more {
					return
				}
				for i := 0; i < len(refreshList); {
					if refreshList[i].sqsReceipt == task.sqsReceipt {
						refreshList = append(refreshList[:i], refreshList[i+1:]...)
						continue
					}
					i++
				}
			default:
			}

			if len(refreshList) > 0 {
				Debug.Printf("SQS in refresh list: %d\n", len(refreshList))
				time.Sleep(time.Duration(60/(1+len(refreshList))) * time.Second)
			}
		}
	}(conf, &wg, refreshMessageVisibilityChan, stopRefreshMessageVisibilityChan)

	for workerIndex := 0; workerIndex < int(*conf.parallelTransfers); workerIndex++ {

		// UPLOAD TO S3 WORKERS
		wg.Add(1)
		go func(conf config, wg *sync.WaitGroup, workerIndex int) {
			defer Debug.Printf("Worker finished: Upload (%d)", workerIndex)
			defer wg.Done()
			defer close(deleteSqsChan)
			Debug.Printf("Worker started: Upload (%d)", workerIndex)

			for task := range uploadToS3Chan {
				if err := S3Upload(
					&task.destBucket,
					&task.destPath,
					&task.destRegion,
					&task.localPath,
				); err == nil {
					deleteSqsChan <- task
					if err := os.Remove(task.localPath); err != nil {
						Error.Printf("Failed remove local file: %s, %v (%d)", task.localPath, err, workerIndex)
					}
				}

			}
		}(conf, &wg, workerIndex)

		// DOWNLOAD FROM S3 WORKERS
		wg.Add(1)
		go func(conf config, wg *sync.WaitGroup, workerIndex int) {
			defer Debug.Printf("Worker finished: Download (%d)", workerIndex)
			defer wg.Done()
			defer close(uploadToS3Chan)
			Debug.Printf("Worker started: Download (%d)", workerIndex)

			for task := range downloadFromS3Chan {

				s3Retry, errS3Download := S3Download(
					&task.sourceBucket,
					&task.sourcePath,
					&task.sourceRegion,
					&task.localPath,
				)
				if errS3Download == nil {
					Debug.Printf("Downloaded OK: %v (%d)", task.sourcePath, workerIndex)
					uploadToS3Chan <- task

				} else {
					Error.Printf(
						"Failed to download from S3: s3://%s/%s (retry_later=%v) (%d)",
						task.sourceBucket,
						task.sourcePath,
						s3Retry,
						workerIndex,
					)
					if s3Retry == true {
						stopRefreshMessageVisibilityChan <- task
					} else {
						deleteSqsChan <- task
					}

				}

			}
		}(conf, &wg, workerIndex)
	}

	// POLL SQS LOOP
	pathTemplate := template.Must(template.New("path").Parse(*conf.targetPath))
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

				// TODO: make this more useful
				pathMap := pathStruct{
					OriginalPath: msgRecord.S3.Object.Key,
					KeyParts:     strings.Split(msgRecord.S3.Object.Key, "/"),
				}

				pathBuf := &bytes.Buffer{}
				err := pathTemplate.ExecuteTemplate(pathBuf, "path", pathMap)
				if err != nil {
					Error.Printf("Failed to execute targetpath template")
					continue
				}

				item := copyTaskItem{
					s3msgs.ReceiptHandle,
					msgRecord.S3.Bucket.Name,
					msgRecord.AwsRegion,
					*conf.s3Name,
					*conf.s3Region,
					msgRecord.S3.Object.Key,
					pathBuf.String(),
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
