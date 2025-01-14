// Contains a simple key-value store for saving object data with a key. Persistence takes place via a simple
// write-ahead-log. All data is also stored in a skip-list in the memory.
package memtable

import (
	"context"
	"github.com/mwildt/goodb/base"
	"github.com/mwildt/goodb/codecs"
	"github.com/mwildt/goodb/messagelog"
	"github.com/mwildt/goodb/skiplist"
	"golang.org/x/exp/constraints"
	"log"
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

// Memtable A simple memtable implementation using a skiplist in-memory index and write ahead log for persistence
type Memtable[K constraints.Ordered, V any] struct {
	name              string
	index             *skiplist.SkipList[K, V]
	log               *messagelog.MessageLog[memtableMessage[K, []byte]]
	mutex             *sync.Mutex
	frs               *fileRotationSequence
	compactThreshold  int
	enableAutoCompact bool
	codec             codecs.Codec[V]
}

// CreateMemtable create a new instance of Memtable
func CreateMemtable[K constraints.Ordered, V any](name string, options ...ConfigOption) (*Memtable[K, V], error) {
	config := newConfig(options)
	frs, err := initFileRotationSequence(config.datadir, name, config.logSuffix)
	if err != nil {
		return nil, err
	}

	if len(config.migrations) > 0 {
		if migman, err := NewMigrationManager[K, MigrationObject](name, frs, codecs.NewJsonCodec[MigrationObject](), config.migrations...); err != nil {
			return nil, err
		} else if err = migman.migrate(context.Background()); err != nil {
			return nil, err
		}
	}

	if messageLog, err := messagelog.NewMessageLog[memtableMessage[K, []byte]](frs.CurrentFilename()); err != nil {
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
			codec:             codecs.NewJsonCodec[V](),
		}
		return repo, repo.init()
	}
}

func (mt *Memtable[K, V]) init() error {

	n, err := mt.log.Open(func(ctx context.Context, message memtableMessage[K, []byte]) error {
		switch message.Type {
		case write:
			if decoded, err := mt.codec.Decode(message.Value); err != nil {
				return err
			} else {
				mt.index.Set(message.Key, decoded)
			}
		case delete:
			mt.index.Delete(message.Key)
		}
		return nil
	})
	log.Printf("Memtable loaded %d records from %s\n", n, mt.log.GetFilename())
	return err
}

// Set e key value pair. Existing entries will be replaced
func (mt *Memtable[K, V]) Set(ctx context.Context, key K, value V) (result V, err error) {
	if encoded, err := mt.codec.Encode(value); err != nil {
		return result, err
	} else {
		entry := memtableMessage[K, []byte]{write, key, encoded}
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
}

// Get finds an existing element
func (mt *Memtable[K, V]) Get(key K) (value V, found bool) {
	return mt.index.Get(key)
}

// Delete removes an existing element by key and returns true if one was deleted
func (mt *Memtable[K, V]) Delete(ctx context.Context, key K) (bool, error) {
	entry := memtableMessage[K, []byte]{delete, key, []byte{}}
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

	if mLog, err := messagelog.NewMessageLog[memtableMessage[K, []byte]](mt.frs.NextFilename()); err != nil {
		return err
	} else if _, err = mLog.Open(messagelog.Noop[memtableMessage[K, []byte]]()); err != nil {
		return err
	} else {
		entries := mt.index.Entries()
		for _, entry := range entries {
			if encoded, err := mt.codec.Encode(entry.Value); err != nil {
				return err
			} else {
				message := memtableMessage[K, []byte]{write, entry.Key, encoded}
				if err := mLog.Append(context.Background(), message); err != nil {
					return err
				}
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
