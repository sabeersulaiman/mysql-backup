package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"strings"
)

type Database struct {
	Name          string   `json:"name"`
	Username      string   `json:"username"`
	Password      string   `json:"password"`
	Times         []string `json:"times"`
	RetentionDays int      `json:"retentionDays"`
}

type Config struct {
	CustomerName string     `json:"customerName"`
	MysqlBinPath string     `json:"mysqlBinPath"`
	S3           S3Info     `json:"s3"`
	Databases    []Database `json:"databases"`
}

type S3Info struct {
	BucketName string `json:"bucketName"`
	AccessKey  string `json:"accessKey"`
	SecretKey  string `json:"secretKey"`
	Region     string `json:"region"`
}

var (
	logFilePath  = "./config.json"
	config       Config
	configLoaded = false
)

func loadConfig() {
	log.Printf("Loading Config File: %s\n", logFilePath)
	dat, err := ioutil.ReadFile(logFilePath)

	if err != nil {
		log.Fatalf("failed to read the config file %v\n", err)
	}

	je := json.Unmarshal(dat, &config)

	if je != nil {
		log.Fatalf("failed while parsing the json file %v\n", je)
	}

	log.Printf("loaded config file successfully\n")
	validateConfig()
	configLoaded = true
}

func GetConfig() Config {
	if !configLoaded {
		loadConfig()
	}

	return config
}

func IsStringEmpty(s string) bool {
	return len(strings.Trim(s, " ")) == 0
}

func validateConfig() {
	if IsStringEmpty(config.CustomerName) {
		log.Fatalf("ERROR: config does not contain a valid customer name")
	}

	if IsStringEmpty(config.S3.BucketName) {
		log.Fatalf("ERROR: cannot find a valid S3 bucket name")
	}

	if IsStringEmpty(config.S3.AccessKey) {
		log.Fatalf("ERROR: cannot find a valid S3 Access Key")
	}

	if IsStringEmpty(config.S3.Region) {
		log.Fatalf("ERROR: cannot find a valid S3 Region")
	}

	if IsStringEmpty(config.S3.SecretKey) {
		log.Fatalf("ERROR: cannot find a valid S3 Secret Key")
	}

	for _, db := range config.Databases {
		if IsStringEmpty(db.Name) {
			log.Fatalf("ERROR: cannot find a valid name for DB %s", db.Name)
		}

		if IsStringEmpty(db.Username) {
			log.Fatalf("ERROR: cannot find a valid username for DB %s", db.Name)
		}

		if IsStringEmpty(db.Password) {
			log.Fatalf("ERROR: cannot find a valid password for DB %s", db.Name)
		}

		if len(db.Times) == 0 {
			log.Panicf("ERROR: cannot find a valid backup time for DB %s", db.Name)
		}

		if db.RetentionDays <= 0 {
			log.Fatalf("ERROR: provide valid retention days for DB %s", db.Name)
		}
	}
}
