package memtable

import (
	"fmt"
	"os"
	"path"
	"regexp"
	"strconv"
	"sync"
)

type fileRotationSequence struct {
	basedir      string
	basename     string
	suffix       string
	currentIndex int
	mutex        *sync.Mutex
}

func (seq *fileRotationSequence) CurrentFilename() string {
	return path.Join(
		seq.basedir,
		fmt.Sprintf("%s.%d.%s", seq.basename, seq.currentIndex, seq.suffix),
	)
}

func (seq *fileRotationSequence) NextFilename() string {
	seq.Increase()
	return seq.CurrentFilename()
}

func (seq *fileRotationSequence) Increase() int {
	seq.mutex.Lock()
	defer seq.mutex.Unlock()
	seq.currentIndex++
	return seq.currentIndex
}

func initFileRotationSequence(basedir string, basename string, suffix string) (seq *fileRotationSequence, err error) {
	pattern := regexp.MustCompile(fmt.Sprintf(`^%s\.(\d+)\.%s$`, basename, suffix))

	files, err := os.ReadDir(basedir)
	if err != nil {
		return seq, err
	}

	highestIdx := -1

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		matches := pattern.FindStringSubmatch(file.Name())
		if matches != nil {
			idx, err := strconv.Atoi(matches[1])
			if err != nil {
				return seq, fmt.Errorf("Fehler beim Konvertieren von idx: %w", err)
			}

			if idx > highestIdx {
				highestIdx = idx
			}
		}
	}
	if highestIdx < 0 {
		highestIdx = 0
	}
	return &fileRotationSequence{basedir, basename, suffix, highestIdx, &sync.Mutex{}}, nil
}
