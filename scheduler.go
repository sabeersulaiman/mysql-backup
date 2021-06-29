package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/go-co-op/gocron"
)

func SetupDatabaseBackupSchedulers(dbs []Database) *gocron.Scheduler {
	log.Printf("Starting schedulers for %d databases\n", len(dbs))

	// we are starting the scheduler in local timezone
	scheduler := gocron.NewScheduler(time.Local)

	for _, db := range dbs {
		for _, time := range db.Times {
			scheduler.Every(1).Day().At(time).Do(
				func() { RunBackupForDb(db) },
			)
		}
	}

	return scheduler
}

func RunBackupForDb(db Database) error {
	dumpFile, err := CreateCompressedBackup(&db)

	if err != nil {
		log.Panicf("Failed to backup the database %v\n", err)
		return err
	}

	log.Printf("Backup success, file saved to %s\n", dumpFile)

	defer os.Remove(dumpFile)

	log.Printf("Uploading file %s to S3\n", dumpFile)

	err = UploadBackupToS3(dumpFile, &db)
	if err != nil {
		log.Panicf("Failed to upload to S3 with error: %v", err)
		return err
	}

	FindAndRemoveExpiredBackups(
		fmt.Sprintf("/%s", db.Name),
		db.RetentionDays,
	)

	return nil
}
