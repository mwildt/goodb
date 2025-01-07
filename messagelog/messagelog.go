package messagelog

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
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
	encoding     *base64.Encoding
	messageCount int
}

func NewMessageLog[V any](filename string) (log *MessageLog[V], err error) {
	if file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644); err != nil {
		return log, err
	} else {
		return &MessageLog[V]{
			file:         file,
			mutex:        &sync.Mutex{},
			encoding:     base64.RawStdEncoding,
			messageCount: 0,
		}, nil
	}
}

func (log *MessageLog[V]) Open(consumer MessageConsumer[V]) (writeCount int, err error) {
	if writeCount, err = log.readAll(context.Background(), consumer); err != nil {
		fmt.Printf("MessageLog::Open mit error %s", err.Error())
		return writeCount, err
	} else {
		log.messageCount = writeCount
		return writeCount, err
	}
}

func (log *MessageLog[V]) encodeMessage(message V) (data []byte, err error) {
	return B64JsonEncoder[V](message)
}

func (log *MessageLog[V]) decodeMessage(data []byte) (message V, err error) {
	return B64JsonDecoder[V](data)
}

func (log *MessageLog[V]) Append(_ context.Context, message V) (err error) {
	log.mutex.Lock()
	defer log.mutex.Unlock()
	if encoded, err := log.encodeMessage(message); err != nil {
		return err
	} else {
		buffLenBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(buffLenBytes, uint32(len(encoded)))
		if _, err := log.file.Write(buffLenBytes); err != nil {
			return err
		} else if _, err := log.file.Write(encoded); err != nil {
			return err
		} else {
			log.messageCount = log.messageCount + 1
		}
	}
	return err
}

func (log *MessageLog[V]) readAll(ctx context.Context, consumer MessageConsumer[V]) (count int, err error) {

	for {
		lenBytes := make([]byte, 4)
		if _, err := io.ReadFull(log.file, lenBytes); err != nil {
			if err == io.EOF {
				return count, nil
			}
			return count, err
		}
		dataLen := binary.LittleEndian.Uint32(lenBytes)
		dataBuffer := make([]byte, int(dataLen))
		if _, err := io.ReadFull(log.file, dataBuffer); err != nil {
			return count, err
		} else if message, err := log.decodeMessage(dataBuffer); err != nil {
			return count, err
		} else if err = consumer(ctx, message); err != nil {
			return count, err
		} else {
			count = count + 1
		}
	}
	return count, err
}

func (log *MessageLog[V]) Close() error {
	log.file.Sync()
	return log.file.Close()
}

func (log *MessageLog[V]) Delete() {
	os.Remove(log.file.Name())
}

func (log *MessageLog[V]) MessageCount() int {
	return log.messageCount
}
