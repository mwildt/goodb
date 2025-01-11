// Contains a simple implementation of a write-ahead log for writing and reading messages.
// Reading takes place once when the log is opened.
package messagelog

import (
	"context"
	"encoding/binary"
	"github.com/mwildt/goodb/codecs"
	"io"
	"log"
	"os"
	"sync"
)

type MessageConsumer[V any] func(_ context.Context, _ V) error

func Noop[V any]() MessageConsumer[V] {
	return func(_ context.Context, _ V) error {
		return nil
	}
}

type MessageLog[V any] struct {
	file         *os.File
	mutex        *sync.Mutex
	messageCount int
	codec        codecs.Codec[V]
}

func NewMessageLog[V any](filename string) (log *MessageLog[V], err error) {
	if file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644); err != nil {
		return log, err
	} else {
		return &MessageLog[V]{
			file:         file,
			mutex:        &sync.Mutex{},
			messageCount: 0,
			codec:        codecs.NewBase64JsonCodec[V](),
		}, nil
	}
}

func (mlog *MessageLog[V]) Open(consumer MessageConsumer[V]) (writeCount int, err error) {
	if writeCount, err = mlog.readAll(context.Background(), consumer); err != nil {
		log.Printf("MessageLog::Open mit error %s", err.Error())
		return writeCount, err
	} else {
		mlog.messageCount = writeCount
		return writeCount, err
	}
}

func (mlog *MessageLog[V]) Append(_ context.Context, message V) (err error) {
	mlog.mutex.Lock()
	defer mlog.mutex.Unlock()
	if encoded, err := mlog.codec.Encode(message); err != nil {
		return err
	} else {
		buffLenBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(buffLenBytes, uint32(len(encoded)))
		if _, err := mlog.file.Write(buffLenBytes); err != nil {
			return err
		} else if _, err := mlog.file.Write(encoded); err != nil {
			return err
		} else {
			mlog.messageCount = mlog.messageCount + 1
		}
	}
	return err
}

func (mlog *MessageLog[V]) readAll(ctx context.Context, consumer MessageConsumer[V]) (count int, err error) {

	for {
		lenBytes := make([]byte, 4)
		if _, err := io.ReadFull(mlog.file, lenBytes); err != nil {
			if err == io.EOF {
				return count, nil
			}
			return count, err
		}
		dataLen := binary.LittleEndian.Uint32(lenBytes)
		dataBuffer := make([]byte, int(dataLen))
		if _, err := io.ReadFull(mlog.file, dataBuffer); err != nil {
			return count, err
		} else if message, err := mlog.codec.Decode(dataBuffer); err != nil {
			return count, err
		} else if err = consumer(ctx, message); err != nil {
			return count, err
		} else {
			count = count + 1
		}
	}
	return count, err
}

func (mlog *MessageLog[V]) Close() error {
	mlog.file.Sync()
	return mlog.file.Close()
}

func (mlog *MessageLog[V]) Delete() {
	os.Remove(mlog.file.Name())
}

func (mlog *MessageLog[V]) MessageCount() int {
	return mlog.messageCount
}

func (mlog *MessageLog[V]) GetFilename() string {
	return mlog.file.Name()
}
