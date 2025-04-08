package storage

import (
	"github.com/gabriel-vasile/mimetype"
	"github.com/opdss/common/contracts/storage"
	"os"
	"time"
)

func LastModified(file string) (time.Time, error) {
	fileInfo, err := os.Stat(file)
	if err != nil {
		return time.Time{}, err
	}
	return fileInfo.ModTime(), nil
}

func Size(file string) (int64, error) {
	fi, err := os.Stat(file)
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func MimeType(file string) (string, error) {
	mtype, err := mimetype.DetectFile(file)
	if err != nil {
		return "", err
	}

	return mtype.String(), nil
}

func getPageSize(maxKeys int32) int32 {
	if maxKeys > 0 && maxKeys <= storage.MaxFileNum {
		return maxKeys
	}
	return storage.DefaultFileNum
}
