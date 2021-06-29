package main

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"time"
)

func CreateCompressedBackup(db *Database) (string, error) {
	// run mysqldump to get the output file
	fileName, err := runMysqlDump(db)

	if err != nil {
		return "", err
	}

	// remove the sql file once done
	defer os.Remove(fileName)

	// compress the output sql file
	compressedFile, ce := compressFile(fileName)
	if ce != nil {
		return "", ce
	}

	return compressedFile, nil
}

func compressFile(file string) (string, error) {
	// read the requested file
	readFile, re := os.Open(file)
	if re != nil {
		return "", re
	}

	// create a file for writing the tar
	writeFile, we := os.Create(file + ".tar.gz")
	if we != nil {
		return "", we
	}

	// get the file stat
	stat, err := readFile.Stat()
	if err != nil {
		return "", err
	}

	gzipWriter := gzip.NewWriter(writeFile)
	defer gzipWriter.Close()

	// complete the write
	writer := tar.NewWriter(gzipWriter)
	defer writer.Close()

	// write the header
	err = writer.WriteHeader(
		&tar.Header{
			Name:    stat.Name(),
			Size:    stat.Size(),
			Mode:    int64(stat.Mode()),
			ModTime: stat.ModTime(),
		},
	)

	if err != nil {
		return "", err
	}

	// write the actual tar content
	_, err = io.Copy(writer, readFile)

	if err != nil {
		return "", err
	}

	return file + ".tar.gz", nil
}

func runMysqlDump(db *Database) (string, error) {
	log.Printf("Starting mysql-dump for DB: %s\n", db.Name)

	now := time.Now()
	fileName := fmt.Sprintf("backup_%s_%s.*.sql", db.Name, now.Format("2006-01-02_15:04"))
	file, _ := ioutil.TempFile("", fileName)

	config := GetConfig()
	mysqlDumbBase := config.MysqlBinPath

	if mysqlDumbBase == "" {
		mysqlDumbBase, _ = exec.LookPath("mysqldump")
	}

	defer file.Close()

	// execute the mysql_dump command
	cmd := exec.Command(
		mysqlDumbBase,
		fmt.Sprintf("-u%s", db.Username),
		fmt.Sprintf("-p%s", db.Password),
		db.Name,
	)
	cmd.Stdout = file
	err := cmd.Run()

	if err != nil {
		fmt.Printf("Failed to run mysql dump command with error: %v. Will have to retry the operation.\n", err)
		os.Remove(file.Name())
		return "", errors.New("failed to perform backup operation, please try again")
	}

	return file.Name(), nil
}
