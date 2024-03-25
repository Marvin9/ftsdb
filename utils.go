package main

import (
	"os"
	"path/filepath"
)

func GetIngestionDir() string {
	dir, _ := os.Getwd()
	dir = filepath.Join(dir, "ingestion")
	return dir
}
