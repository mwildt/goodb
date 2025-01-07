// Package memtable: a simple in memory datastructure that represents a key-value store, that is mirrored to disk
// using a write-ahead log
package memtable

import (
	"context"
	"github.com/mwildt/goodb/base"
	"github.com/mwildt/goodb/messagelog"
	"github.com/mwildt/goodb/skiplist"
	"golang.org/x/exp/constraints"
	"sync"
)

type entryType int8

const (
	delete entryType = 0
	write  entryType = 1
)

type memtableMessage[K constraints.Ordered, V any] struct {
	Type  entryType
	Key   K
	Value V
}

type Memtable[K constraints.Ordered, V any] struct {
	name              string
	index             *skiplist.SkipList[K, V]
	log               *messagelog.MessageLog[memtableMessage[K, V]]
	mutex             *sync.Mutex
	frs               *fileRotationSequence
	compactThreshold  int
	enableAutoCompact bool
}

func CreateMemtable[K constraints.Ordered, V any](name string, options ...ConfigOption) (*Memtable[K, V], error) {
	config := newConfig(options)
	if frs, err := initFileRotationSequence(config.datadir, name, config.logSuffix); err != nil {
		return nil, err
	} else if messageLog, err := messagelog.NewMessageLog[memtableMessage[K, V]](frs.CurrentFilename()); err != nil {
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
	_, err := mt.log.Open(func(ctx context.Context, message memtableMessage[K, V]) error {
		switch message.Type {
		case write:
			mt.index.Set(message.Key, message.Value)
		case delete:
			mt.index.Delete(message.Key)
		}
		return nil
	})
	return err
}

func (mt *Memtable[K, V]) Set(ctx context.Context, key K, value V) (result V, err error) {
	entry := memtableMessage[K, V]{write, key, value}
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
	entry := memtableMessage[K, V]{delete, key, v}
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

	if mLog, err := messagelog.NewMessageLog[memtableMessage[K, V]](mt.frs.NextFilename()); err != nil {
		return err
	} else if _, err = mLog.Open(messagelog.Noop[memtableMessage[K, V]]()); err != nil {
		return err
	} else {
		entries := mt.index.Entries()
		for _, entry := range entries {
			err := mLog.Append(context.Background(), memtableMessage[K, V]{write, entry.Key, entry.Value})
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
