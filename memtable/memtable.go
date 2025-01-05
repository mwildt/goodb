package memtable

import (
	"context"
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

type Entry[K constraints.Ordered, V any] struct {
	Key   K
	Value V
}

type Memtable[K constraints.Ordered, V any] struct {
	name  string
	index *skiplist.SkipList[K, V]
	log   *messagelog.MessageLog[Message[K, V]]
	mutex *sync.Mutex
	frs   *fileRotationSequence
}

func CreateMemtable[K constraints.Ordered, V any](datadir string, name string) (*Memtable[K, V], error) {
	if frs, err := initFileRotationSequence(datadir, name, "mt.log"); err != nil {
		return nil, err
	} else if messageLog, err := messagelog.NewMessageLog[Message[K, V]](frs.CurrentFilename()); err != nil {
		return nil, err
	} else {
		repo := &Memtable[K, V]{
			name:  name,
			index: skiplist.NewSkipList[K, V](4),
			log:   messageLog,
			mutex: &sync.Mutex{},
			frs:   frs,
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

func (mt *Memtable[K, V]) Entries() <-chan Entry[K, V] {
	keys := mt.Keys()
	values := mt.Values()
	result := make(chan Entry[K, V])
	ok := true
	for ok {
		key, ok1 := <-keys
		value, ok2 := <-values
		ok = ok1 && ok2
		if ok {
			result <- Entry[K, V]{key, value}
		}
	}
	return result
}

func (mt *Memtable[K, V]) Size() int {
	return mt.index.Size()
}

func (mt *Memtable[K, V]) Close() error {
	return mt.log.Close()
}
