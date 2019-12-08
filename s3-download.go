package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"os"
)

// Returns retryInFuture{bool}, and error
func S3Download(bucket *string, item *string, region *string, filename *string) (bool, error) {

	file, err := os.Create(*filename)
	if err != nil {
		return true, fmt.Errorf("S3 Download Error: Unable to create local file: %v, %v", *filename, err)
	}
	defer file.Close()

	Debug.Printf("Downloading s3://%s/%s (%s) ==> %s\n\n", *bucket, *item, *region, *filename)

	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String(*region)},
	)

	downloader := s3manager.NewDownloader(sess)

	numBytes, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(*bucket),
			Key:    aws.String(*item),
		})
	if err != nil {

		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket:
				return false, fmt.Errorf("S3 Download Error: Bucket not exist: %s (%v)", *bucket, err)
			case s3.ErrCodeNoSuchKey:
				return false, fmt.Errorf("S3 Download Error: File not exist: %s (%v)", *item, err)
			}
		}
		return true, fmt.Errorf("S3 Download Error: s3://%s/%s (%v)", *bucket, *item, err)
	}

	Debug.Printf("Downloaded %d bytes", numBytes)

	return false, nil
}
