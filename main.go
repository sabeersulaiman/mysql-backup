package main

import (
	"log"
)

func main() {
	// setup logging
	log.SetPrefix("mysql_backup: ")

	// load config
	config := GetConfig()

	// set up the schedulers
	scheduler := SetupDatabaseBackupSchedulers(config.Databases)

	// start the scheduler
	scheduler.StartBlocking()
}
