package testutils

import (
	"log"
	"os"
)

func RunWithTempDir(dirPath string, runnable func(string)) {

	if err := os.MkdirAll(dirPath, 0755); err != nil {
		log.Fatalf("failed to create directory: %s", err.Error())
	}

	defer func() {
		if err := os.RemoveAll(dirPath); err != nil {
			log.Fatalf("warning: failed to remove directory: %s\n", err)
		}
	}()

	runnable(dirPath)
}
