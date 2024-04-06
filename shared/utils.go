package shared

import (
	"os"
	"path/filepath"
)

func GetIngestionDir() string {
	dir, _ := os.Getwd()
	dir = filepath.Join(dir, "ingestion")
	return dir
}

func GetPromIngestionDir() string {
	dir, _ := os.Getwd()
	dir = filepath.Join(dir, "prom-ingestion")
	if err := os.MkdirAll(dir, 0777); err != nil {
		NoErr(err)
	}
	return dir
}

func NoErr(err error) {
	if err != nil {
		panic(err)
	}
}
