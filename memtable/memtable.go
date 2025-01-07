package memtable

import (
	"context"
	"github.com/mwildt/goodb/base"
	"github.com/mwildt/goodb/messagelog"
	"github.com/mwildt/goodb/skiplist"
	"golang.org/x/exp/constraints"
	"sync"
)

type EntryType int8

const (
	DELETE EntryType = 0
	WRITE  EntryType = 1
)

type Message[K constraints.Ordered, V any] struct {
	Type  EntryType
	Key   K
	Value V
}

type Memtable[K constraints.Ordered, V any] struct {
	name              string
	index             *skiplist.SkipList[K, V]
	log               *messagelog.MessageLog[Message[K, V]]
	mutex             *sync.Mutex
	frs               *fileRotationSequence
	compactThreshold  int
	enableAutoCompact bool
}

type memtableConfiguration struct {
	datadir           string
	logSuffix         string
	compactThreshold  int
	enableAutoCompact bool
}

type ConfigOption func(*memtableConfiguration)

func newConfig(options []ConfigOption) memtableConfiguration {
	config := memtableConfiguration{
		datadir:           "./data",
		logSuffix:         "mtlog",
		compactThreshold:  100,
		enableAutoCompact: true,
	}
	for _, opt := range options {
		opt(&config)
	}

	return config
}

func WithDatadir(value string) ConfigOption {

	return func(c *memtableConfiguration) {
		c.datadir = value
	}
}

func WithCompactThreshold(value int) ConfigOption {
	return func(c *memtableConfiguration) {
		c.compactThreshold = value
	}
}

func WithDisableAutoCompaction() ConfigOption {
	return func(c *memtableConfiguration) {
		c.enableAutoCompact = false
	}
}

func CreateMemtable[K constraints.Ordered, V any](name string, options ...ConfigOption) (*Memtable[K, V], error) {
	config := newConfig(options)
	if frs, err := initFileRotationSequence(config.datadir, name, config.logSuffix); err != nil {
		return nil, err
	} else if messageLog, err := messagelog.NewMessageLog[Message[K, V]](frs.CurrentFilename()); err != nil {
		return nil, err
	} else {
		repo := &Memtable[K, V]{
			name:              name,
			index:             skiplist.NewSkipList[K, V](),
			log:               messageLog,
			mutex:             &sync.Mutex{},
			frs:               frs,
			compactThreshold:  config.compactThreshold,
			enableAutoCompact: config.enableAutoCompact,
		}
		return repo, repo.init()
	}
}

func (mt *Memtable[K, V]) init() error {
	_, err := mt.log.Open(func(ctx context.Context, message Message[K, V]) error {
		switch message.Type {
		case WRITE:
			mt.index.Set(message.Key, message.Value)
		case DELETE:
			mt.index.Delete(message.Key)
		}
		return nil
	})
	return err
}

func (mt *Memtable[K, V]) Set(ctx context.Context, key K, value V) (result V, err error) {
	entry := Message[K, V]{WRITE, key, value}
	if err := mt.log.Append(ctx, entry); err != nil {
		return value, err
	} else {
		mt.index.Set(key, value)
		defer func() {
			go mt.autoCompaction()
		}()
		return value, err
	}
}

func (mt *Memtable[K, V]) Get(key K) (value V, found bool) {
	return mt.index.Get(key)
}

func (mt *Memtable[K, V]) Delete(ctx context.Context, key K) (bool, error) {
	var v V
	entry := Message[K, V]{DELETE, key, v}
	if err := mt.log.Append(ctx, entry); err != nil {
		return false, err
	} else {
		return mt.index.Delete(key), nil
	}
}

func (mt *Memtable[K, V]) Keys() <-chan K {
	return mt.index.Keys()
}

func (mt *Memtable[K, V]) Values() <-chan V {
	return mt.index.Values()
}

func (mt *Memtable[K, V]) Entries() []base.Entry[K, V] {
	return mt.index.Entries()
}

func (mt *Memtable[K, V]) Size() int {
	return mt.index.Size()
}

func (mt *Memtable[K, V]) Close() error {
	return mt.log.Close()
}

func (mt *Memtable[K, V]) autoCompaction() (err error) {
	if !mt.enableAutoCompact {
		return nil
	}
	if mt.log.MessageCount() >= mt.index.Size()+mt.compactThreshold {
		return mt.compact()
	}
	return nil
}

func (mt *Memtable[K, V]) compact() (err error) {

	mt.mutex.Lock()
	defer mt.mutex.Unlock()

	if mLog, err := messagelog.NewMessageLog[Message[K, V]](mt.frs.NextFilename()); err != nil {
		return err
	} else if _, err = mLog.Open(messagelog.Noop[Message[K, V]]()); err != nil {
		return err
	} else {
		entries := mt.index.Entries()
		for _, entry := range entries {
			err := mLog.Append(context.Background(), Message[K, V]{WRITE, entry.Key, entry.Value})
			if err != nil {
				return err
			}
		}

		oldStore := mt.log
		mt.log = mLog

		defer func() {
			oldStore.Close()
			oldStore.Delete()
		}()
		return nil
	}
}
