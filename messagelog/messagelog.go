package messagelog

import (
	"context"
	"encoding/binary"
	"github.com/mwildt/goodb/base"
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
	decoder      base.Decoder[V]
	encoder      base.Encoder[V]
}

func NewMessageLog[V any](filename string) (log *MessageLog[V], err error) {
	if file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644); err != nil {
		return log, err
	} else {
		return &MessageLog[V]{
			file:         file,
			mutex:        &sync.Mutex{},
			messageCount: 0,
			encoder:      base.B64JsonEncoder[V],
			decoder:      base.B64JsonDecoder[V],
		}, nil
	}
}

func NewMessageLogEncode[V any](filename string, encoder base.Encoder[V], decoder base.Decoder[V]) (log *MessageLog[V], err error) {
	if file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644); err != nil {
		return log, err
	} else {
		return &MessageLog[V]{
			file:         file,
			mutex:        &sync.Mutex{},
			messageCount: 0,
			encoder:      encoder,
			decoder:      decoder,
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

func (mlog *MessageLog[V]) encodeMessage(message V) (data []byte, err error) {
	return mlog.encoder(message)
}

func (mlog *MessageLog[V]) decodeMessage(data []byte) (message V, err error) {
	return mlog.decoder(data)
}

func (mlog *MessageLog[V]) Append(_ context.Context, message V) (err error) {
	mlog.mutex.Lock()
	defer mlog.mutex.Unlock()
	if encoded, err := mlog.encodeMessage(message); err != nil {
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
		} else if message, err := mlog.decodeMessage(dataBuffer); err != nil {
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
