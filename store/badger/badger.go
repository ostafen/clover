package badger

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/ostafen/clover/v2/store"
)

type badgerStore struct {
	db     *badger.DB
	chWg   sync.WaitGroup
	chQuit chan struct{}
}

func (store *badgerStore) Begin(update bool) (store.Tx, error) {
	tx := store.db.NewTransaction(update)
	return &badgerTx{Txn: tx}, nil
}

func (store *badgerStore) Close() error {
	store.stopGC()
	return store.db.Close()
}

type badgerTx struct {
	*badger.Txn
}

func (tx *badgerTx) Set(key, value []byte) error {
	return tx.Txn.Set(key, value)
}

func getItemValue(item *badger.Item) ([]byte, error) {
	var value []byte
	err := item.Value(func(val []byte) error {
		value = val
		return nil
	})
	return value, err
}

func (tx *badgerTx) Get(key []byte) ([]byte, error) {
	item, err := tx.Txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}
	return getItemValue(item)
}

func (tx *badgerTx) Commit() error {
	return tx.Txn.Commit()
}

func (tx *badgerTx) Rollback() error {
	tx.Txn.Discard()
	return nil
}

func (tx *badgerTx) Cursor(forward bool) (store.Cursor, error) {
	opts := badger.DefaultIteratorOptions
	opts.Reverse = !forward
	return &badgerCursor{it: tx.NewIterator(opts)}, nil
}

type badgerCursor struct {
	it *badger.Iterator
}

func (cursor *badgerCursor) Seek(key []byte) error {
	cursor.it.Seek(key)
	return nil
}

func (cursor *badgerCursor) Next() {
	cursor.it.Next()
}

func (cursor *badgerCursor) Valid() bool {
	return cursor.it.Valid()
}

func (cursor *badgerCursor) Item() (store.Item, error) {
	item := cursor.it.Item()

	value, err := getItemValue(item)
	return store.Item{Key: item.Key(), Value: value}, err
}

func (cursor *badgerCursor) Close() error {
	cursor.it.Close()
	return nil
}

func Open(opts badger.Options) (store.Store, error) {
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	store := &badgerStore{
		db:     db,
		chQuit: make(chan struct{}, 1),
	}
	store.startGC()
	return store, nil
}

const (
	GCReclaimInterval = time.Minute * 5
	GCDiscardRatio    = 0.5
)

func (store *badgerStore) startGC() {
	store.chWg.Add(1)

	go func() {
		defer store.chWg.Done()

		ticker := time.NewTicker(GCReclaimInterval)
		defer ticker.Stop()

		for {
			select {
			case <-store.chQuit:
				return

			case <-ticker.C:
				err := store.db.RunValueLogGC(GCDiscardRatio)
				if err != nil && errors.Is(err, badger.ErrNoRewrite) {
					log.Fatalf("RunValueLogGC(): %s\n", err.Error())
				}
			}
		}
	}()
}

func (store *badgerStore) stopGC() {
	store.chQuit <- struct{}{}
	store.chWg.Wait()
	close(store.chQuit)
}
