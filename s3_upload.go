package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var (
	AWSSession *session.Session
)

func UploadBackupToS3(fileName string, db *Database) error {
	sess := getS3Session()
	uploader := s3manager.NewUploader(sess)

	f, err := os.Open(fileName)
	if err != nil {
		return err
	}

	stat, err := f.Stat()
	if err != nil {
		return err
	}

	res, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(config.S3.BucketName),
		Key: aws.String(
			createCustomerFileName(
				fmt.Sprintf("%s/%s", db.Name, stat.Name()),
			)),
		Body: f,
	})

	if err != nil {
		return err
	}

	log.Printf("uploaded file %s to S3 location: %s\n", fileName, res.Location)

	return nil
}

func DeleteFile(key string) error {
	sess := getS3Session()
	svc := s3.New(sess)
	config := GetConfig()

	log.Printf("Deleting file with key %s from S3", key)

	_, err := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: &config.S3.BucketName,
		Key:    aws.String(key),
	})

	if err != nil {
		log.Panicf("Error occurred while deleting file : %s, %v", key, err)
		return err
	}

	return nil
}

func GetAllAvailableFilesInPath(path string) (*s3.ListObjectsOutput, error) {
	sess := getS3Session()
	svc := s3.New(sess)
	config := GetConfig()

	objs, err := svc.ListObjects(&s3.ListObjectsInput{
		Bucket: &config.S3.BucketName,
		Prefix: aws.String(fmt.Sprintf("%s%s", config.CustomerName, path)),
	})

	if err != nil {
		log.Panicf("Failed to list objects under path %s, error %v\n", path, err)
		return nil, err
	}

	return objs, nil
}

func FindAndRemoveExpiredBackups(path string, retentionDays int) error {
	// load all objects of the path
	objs, err := GetAllAvailableFilesInPath(path)
	if err != nil {
		return err
	}

	retentionDate := calculateRetentionDate(retentionDays)

	for _, obj := range objs.Contents {
		if obj.LastModified.Before(retentionDate) {
			err := DeleteFile(*obj.Key)

			if err != nil {
				log.Printf("Failed while deleting the file %s\n", *obj.Key)
				return err
			}
		}
	}

	return nil
}

//////////////////////////////////////////////////////////// unexported methods

func calculateRetentionDate(retentionDays int) time.Time {
	now := time.Now()
	return now.AddDate(0, 0, retentionDays*-1)
}

func createCustomerFileName(key string) string {
	config := GetConfig()
	return fmt.Sprintf("%s/%s", config.CustomerName, key)
}

func getS3Session() *session.Session {
	if AWSSession != nil {
		return AWSSession
	}

	// create a new session
	createS3Session()
	return AWSSession
}

func createS3Session() error {
	config := GetConfig()

	// TODO: make sure that all configs are present

	// s3 uploader
	sess, err := session.NewSession(
		&aws.Config{
			Region:      aws.String(config.S3.Region),
			Credentials: credentials.NewStaticCredentials(config.S3.AccessKey, config.S3.SecretKey, ""),
		})

	if err != nil {
		return err
	}

	AWSSession = sess
	return nil
}
