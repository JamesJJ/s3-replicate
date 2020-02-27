[![Docker Automated build](https://img.shields.io/docker/cloud/automated/jamesjj/s3-replicate)](https://hub.docker.com/r/jamesjj/s3-replicate/)
[![Docker Automated build](https://img.shields.io/docker/cloud/build/jamesjj/s3-replicate)](https://hub.docker.com/r/jamesjj/s3-replicate/)

# S3 Replicate

Copies files from S3 to S3, with support for templated destination path and filename.

## Usage

Configuration can be specified using command line flags or `S3TOS3_*` environment variables.

```
  -bucket string
    	Name of the S3 bucket to store TSV files [MANDATORY] [S3TOS3_BUCKET]

  -bucketregion string
    	AWS region of S3 bucket [MANDATORY] [S3TOS3_BUCKETREGION]

  -emptypolls int
    	How many consecutive times to poll SQS and receive zero messages before exiting, 1+ [S3TOS3_EMPTYPOLLS] (default 3)

  -parallel int
    	How many files to transfer concurrently [S3TOS3_PARALLEL] (default 16)

  -pollmessages int
    	SQS maximum messages per poll, 1-10 [S3TOS3_POLLMESSAGES] (default 10)

  -polltimeout int
    	SQS slow poll timeout, 1-20 [S3TOS3_POLLTIMEOUT] (default 18)

  -pollbackoff int
        If no SQS messages sequentially, wait for this number of seconds before polling again [S3TOS3_POLLBACKOFF] (default 0)

  -sqs string
    	Name of the SQS queue to poll [MANDATORY] [S3TOS3_SQS]

  -sqsregion string
    	AWS region of SQS queue [MANDATORY] [S3TOS3_SQSREGION]

  -targetpath string
    	Target path go-template [S3TOS3_TARGETPATH] (default "{{ .OriginalPath }}")

  -verbose
    	Show detailed information during run [S3TOS3_VERBOSE]
```

