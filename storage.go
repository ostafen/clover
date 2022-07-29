package clover

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	badger "github.com/dgraph-io/badger/v3"
	"github.com/ostafen/clover/v2/internal"
)

var ErrDocumentNotExist = errors.New("no such document")
var ErrDuplicateKey = errors.New("duplicate key")

type docConsumer func(doc *Document) error

// StorageEngine represents the persistance layer and abstracts how collections are stored.
type StorageEngine interface {
	Open(path string, c *Config) error
	Close() error

	CreateCollection(name string) error
	ListCollections() ([]string, error)
	DropCollection(name string) error
	HasCollection(name string) (bool, error)
	Count(q *Query) (int, error)
	FindAll(q *Query) ([]*Document, error)
	FindById(collectionName string, id string) (*Document, error)
	UpdateById(collectionName string, docId string, updater func(doc *Document) *Document) error
	DeleteById(collectionName string, id string) error
	IterateDocs(q *Query, consumer docConsumer) error
	Insert(collection string, docs ...*Document) error
	Update(q *Query, updater func(doc *Document) *Document) error
	Delete(q *Query) error
	CreateIndex(collection, field string) error
	DropIndex(collection, field string) error
	HasIndex(collection, field string) (bool, error)
	ListIndexes(collection string) ([]string, error)
}

var errStopIteration = errors.New("iteration stop")

type storageImpl struct {
	db     *badger.DB
	conf   *Config
	chQuit chan struct{}
	chWg   sync.WaitGroup
	closed uint32
}

func newDefaultStorageImpl() *storageImpl {
	return &storageImpl{
		chQuit: make(chan struct{}, 1),
	}
}

func (s *storageImpl) startGC() {
	s.chWg.Add(1)

	go func() {
		defer s.chWg.Done()

		ticker := time.NewTicker(s.conf.GCReclaimInterval)
		defer ticker.Stop()

		for {
			select {
			case <-s.chQuit:
				return

			case <-ticker.C:
				err := s.db.RunValueLogGC(s.conf.GCDiscardRatio)
				if err != nil {
					log.Printf("RunValueLogGC(): %s\n", err.Error())
				}
			}
		}
	}()
}

func (s *storageImpl) stopGC() {
	s.chQuit <- struct{}{}
	s.chWg.Wait()
	close(s.chQuit)
}

func (s *storageImpl) Open(path string, c *Config) error {
	if c.InMemory {
		path = ""
	}

	db, err := badger.Open(badger.DefaultOptions(path).WithLoggingLevel(badger.ERROR).WithInMemory(c.InMemory))

	s.db = db
	s.conf = c

	s.startGC()

	return err
}

type collectionMetadata struct {
	Size    int
	Indexes []string
}

func getCollectionKeyPrefix() string {
	return "coll:"
}

func getCollectionKey(name string) string {
	return getCollectionKeyPrefix() + name
}

func (s *storageImpl) CreateCollection(name string) error {
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	ok, err := s.hasCollection(name, txn)
	if err != nil {
		return err
	}

	if ok {
		return ErrCollectionExist
	}

	meta := &collectionMetadata{Size: 0}
	if err := s.saveCollectionMetadata(name, meta, txn); err != nil {
		return err
	}
	return txn.Commit()
}

func (s *storageImpl) DropCollection(name string) error {
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	if err := s.deleteAll(txn, name); err != nil {
		return err
	}

	if err := txn.Delete([]byte(getCollectionKey(name))); err != nil {
		return err
	}

	return txn.Commit()
}

func (s *storageImpl) getCollectionMeta(collection string, txn *badger.Txn) (*collectionMetadata, error) {
	e, err := txn.Get([]byte(getCollectionKey(collection)))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, ErrCollectionNotExist
	}

	m := &collectionMetadata{}
	err = e.Value(func(rawMeta []byte) error {
		return json.Unmarshal(rawMeta, m)
	})
	return m, err
}

func (s *storageImpl) saveCollectionMetadata(collection string, meta *collectionMetadata, txn *badger.Txn) error {
	rawMeta, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return txn.Set([]byte(getCollectionKey(collection)), rawMeta)
}

func (s *storageImpl) getCollectionSize(collection string) (int, error) {
	txn := s.db.NewTransaction(false)
	defer txn.Discard()

	meta, err := s.getCollectionMeta(collection, txn)
	if err != nil {
		return -1, err
	}
	return meta.Size, nil
}

func (s *storageImpl) countCollection(q *Query) (int, error) {
	size, err := s.getCollectionSize(q.collection)
	size -= q.skip

	if size < 0 {
		size = 0
	}

	if q.limit >= 0 && q.limit < size {
		return q.limit, err
	}

	return size, err
}

func (s *storageImpl) Count(q *Query) (int, error) {
	if q.criteria == nil { // simply return the size of the collection in this case
		return s.countCollection(q)
	}

	num := 0
	err := s.IterateDocs(q, func(doc *Document) error {
		num++
		return nil
	})
	return num, err
}

func (s *storageImpl) FindAll(q *Query) ([]*Document, error) {
	docs := make([]*Document, 0)

	err := s.IterateDocs(q, func(doc *Document) error {
		docs = append(docs, doc)
		return nil
	})
	return docs, err
}

func decodeDoc(data []byte) (*Document, error) {
	doc := NewDocument()
	err := internal.Decode(data, &doc.fields)
	return doc, err
}

func encodeDoc(doc *Document) ([]byte, error) {
	return internal.Encode(doc.fields)
}

func getDocumentById(collectionName string, id string, txn *badger.Txn) (*Document, error) {
	item, err := txn.Get([]byte(getDocumentKey(collectionName, id)))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}

	var doc *Document
	err = item.Value(func(data []byte) error {
		d, err := decodeDoc(data)
		doc = d
		return err
	})
	return doc, err
}

func (s *storageImpl) FindById(collectionName string, id string) (*Document, error) {
	txn := s.db.NewTransaction(false)
	defer txn.Discard()

	ok, err := s.hasCollection(collectionName, txn)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, ErrCollectionNotExist
	}

	return getDocumentById(collectionName, id, txn)
}

func getDocumentKeyPrefix(collection string) string {
	return "c:" + collection + ";" + "d:"
}

func getDocumentKey(collection string, id string) string {
	return getDocumentKeyPrefix(collection) + id
}

func (s *storageImpl) addDocToIndexes(txn *badger.Txn, indexes []*indexImpl, doc *Document) error {
	// update indexes
	for _, idx := range indexes {
		fieldVal := doc.Get(idx.fieldName) // missing fields are treated as null

		err := idx.Set(txn, fieldVal, doc.ObjectId(), doc.TTL())
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *storageImpl) Insert(collection string, docs ...*Document) error {
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	meta, err := s.getCollectionMeta(collection, txn)
	if err != nil {
		return err
	}

	indexes := s.getIndexes(collection, meta)

	for _, doc := range docs {
		if err := s.addDocToIndexes(txn, indexes, doc); err != nil {
			return err
		}

		key := []byte(getDocumentKey(collection, doc.ObjectId()))
		_, err = txn.Get(key)
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return ErrDuplicateKey
		}

		if err := saveDocument(doc, key, txn); err != nil {
			return err
		}
	}

	meta.Size += len(docs)
	if err := s.saveCollectionMetadata(collection, meta, txn); err != nil {
		return err
	}

	return txn.Commit()
}

func saveDocument(doc *Document, key []byte, txn *badger.Txn) error {
	if err := validateDocument(doc); err != nil {
		return err
	}

	data, err := encodeDoc(doc)
	if err != nil {
		return err
	}

	e := badger.NewEntry(key, data)

	ttl := doc.TTL()
	if ttl == 0 {
		return nil
	}

	if ttl > 0 {
		e = e.WithTTL(ttl)
	}

	return txn.SetEntry(e)
}

func (s *storageImpl) deleteDocFromIndexes(txn *badger.Txn, indexes []*indexImpl, doc *Document) error {
	for _, idx := range indexes {
		value := doc.Get(idx.fieldName)
		if err := idx.Delete(txn, value, doc.ObjectId()); err != nil {
			return err
		}
	}
	return nil
}

func (s *storageImpl) getDocAndDeleteFromIndexes(txn *badger.Txn, indexes []*indexImpl, collection string, docId string) error {
	if len(indexes) == 0 {
		return nil
	}

	doc, err := getDocumentById(collection, docId, txn)
	if err != nil {
		return err
	}

	if doc == nil {
		return nil
	}

	for _, idx := range indexes {
		value := doc.Get(idx.fieldName)
		if err := idx.Delete(txn, value, doc.ObjectId()); err != nil {
			return err
		}
	}
	return nil
}

func (s *storageImpl) updateIndexesOnDocUpdate(txn *badger.Txn, indexes []*indexImpl, oldDoc, newDoc *Document) error {
	if err := s.deleteDocFromIndexes(txn, indexes, oldDoc); err != nil {
		return err
	}

	if newDoc != nil {
		if err := s.addDocToIndexes(txn, indexes, newDoc); err != nil {
			return err
		}
	}

	return nil
}

type docUpdater func(doc *Document) *Document

func (s *storageImpl) replaceDocs(txn *badger.Txn, q *Query, updater docUpdater) error {
	meta, err := s.getCollectionMeta(q.collection, txn)
	if err != nil {
		return err
	}

	indexes := s.getIndexes(q.collection, meta)

	deletedDocs := 0
	err = s.iterateDocs(txn, q, func(doc *Document) error {
		docKey := []byte(getDocumentKey(q.collection, doc.ObjectId()))
		newDoc := updater(doc)

		if err := s.updateIndexesOnDocUpdate(txn, indexes, doc, newDoc); err != nil {
			return err
		}

		if newDoc == nil {
			deletedDocs++
			return txn.Delete(docKey)
		}

		return saveDocument(newDoc, docKey, txn)
	})

	if err != nil {
		return err
	}

	if deletedDocs > 0 {
		meta.Size -= deletedDocs
		if err := s.saveCollectionMetadata(q.collection, meta, txn); err != nil {
			return err
		}
	}
	return nil
}

func (s *storageImpl) Update(q *Query, updater func(doc *Document) *Document) error {
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	if err := s.replaceDocs(txn, q, updater); err != nil {
		return err
	}
	return txn.Commit()
}

func (s *storageImpl) deleteAll(txn *badger.Txn, collName string) error {
	return s.replaceDocs(txn, NewQuery(collName), func(_ *Document) *Document {
		return nil
	})
}

func (s *storageImpl) Delete(q *Query) error {
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	if err := s.replaceDocs(txn, q, func(_ *Document) *Document { return nil }); err != nil {
		return err
	}
	return txn.Commit()
}

func (s *storageImpl) DeleteById(collName string, id string) error {
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	meta, err := s.getCollectionMeta(collName, txn)
	if err != nil {
		return err
	}

	indexes := s.getIndexes(collName, meta)

	if err := s.getDocAndDeleteFromIndexes(txn, indexes, collName, id); err != nil {
		return err
	}

	if err := txn.Delete([]byte(getDocumentKey(collName, id))); err != nil {
		return err
	}

	meta.Size--
	if err := s.saveCollectionMetadata(collName, meta, txn); err != nil {
		return err
	}
	return txn.Commit()
}

func (s *storageImpl) UpdateById(collectionName string, docId string, updater func(doc *Document) *Document) error {
	return s.db.Update(func(txn *badger.Txn) error {
		meta, err := s.getCollectionMeta(collectionName, txn)
		if err != nil {
			return err
		}

		indexes := s.getIndexes(collectionName, meta)

		docKey := getDocumentKey(collectionName, docId)
		item, err := txn.Get([]byte(docKey))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrDocumentNotExist
		}

		var doc *Document
		err = item.Value(func(value []byte) error {
			d, err := decodeDoc(value)
			doc = d
			return err
		})

		if err != nil {
			return err
		}

		updatedDoc := updater(doc)
		if err := s.updateIndexesOnDocUpdate(txn, indexes, doc, updatedDoc); err != nil {
			return err
		}

		return saveDocument(updatedDoc, []byte(docKey), txn)
	})
}

func (s *storageImpl) Close() error {
	if atomic.CompareAndSwapUint32(&s.closed, 0, 1) {
		s.stopGC()
		return s.db.Close()
	}
	return nil
}

func (s *storageImpl) hasCollection(name string, txn *badger.Txn) (bool, error) {
	_, err := txn.Get([]byte(getCollectionKey(name)))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return false, nil
	}
	return err == nil, err
}

func (s *storageImpl) HasCollection(name string) (bool, error) {
	txn := s.db.NewTransaction(false)
	defer txn.Discard()
	return s.hasCollection(name, txn)
}

func iteratePrefix(prefix []byte, txn *badger.Txn, itemConsumer func(item *badger.Item) error) error {
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		err := itemConsumer(it.Item())

		// do not propagate iteration stop error
		if err == errStopIteration {
			return nil
		}

		if err != nil {
			return err
		}
	}
	return nil
}

var errNoSuitableIndexFound = errors.New("no suitable index found for running provided query")

func (s *storageImpl) iterateDocs(txn *badger.Txn, q *Query, consumer docConsumer) error {
	meta, err := s.getCollectionMeta(q.collection, txn)
	if err != nil {
		return err
	}
	nd := buildQueryPlan(q, s.getIndexes(q.collection, meta), &consumerNode{consumer: consumer})
	return execPlan(nd, txn)
}

func (s *storageImpl) IterateDocs(q *Query, consumer docConsumer) error {
	txn := s.db.NewTransaction(false)
	defer txn.Discard()
	return s.iterateDocs(txn, q, consumer)
}

func (s *storageImpl) ListCollections() ([]string, error) {
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	collections := make([]string, 0)

	prefix := []byte(getCollectionKeyPrefix())
	err := iteratePrefix(prefix, txn, func(item *badger.Item) error {
		key := item.Key()
		collectionName := string(bytes.TrimPrefix(key, prefix))
		collections = append(collections, collectionName)
		return nil
	})
	return collections, err
}

func (s *storageImpl) CreateIndex(collection, field string) error {
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	meta, err := s.getCollectionMeta(collection, txn)
	if err != nil {
		return err
	}

	for i := 0; i < len(meta.Indexes); i++ {
		if meta.Indexes[i] == field {
			return ErrIndexExist
		}
	}

	if meta.Indexes == nil {
		meta.Indexes = make([]string, 0)
	}
	meta.Indexes = append(meta.Indexes, field)

	idx := &indexImpl{
		collectionName: collection,
		fieldName:      field,
	}

	err = s.iterateDocs(txn, NewQuery(collection), func(doc *Document) error {
		value := doc.Get(field)
		return idx.Set(txn, value, doc.ObjectId(), doc.TTL())
	})

	if err != nil {
		return err
	}

	if err := s.saveCollectionMetadata(collection, meta, txn); err != nil {
		return err
	}

	return txn.Commit()
}

func (s *storageImpl) DropIndex(collection, field string) error {
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	meta, err := s.getCollectionMeta(collection, txn)
	if err != nil {
		return err
	}

	index := -1
	for i := 0; i < len(meta.Indexes); i++ {
		if meta.Indexes[i] == field {
			index = i
		}
	}

	if index < 0 {
		return ErrIndexNotExist
	}

	meta.Indexes[index] = meta.Indexes[0]
	meta.Indexes = meta.Indexes[1:]

	idx := &indexImpl{
		collectionName: collection,
		fieldName:      field,
	}

	if err := idx.deleteAll(txn); err != nil {
		return err
	}

	if err := s.saveCollectionMetadata(collection, meta, txn); err != nil {
		return err
	}
	return txn.Commit()
}

func (s *storageImpl) hasIndex(txn *badger.Txn, collection, field string) (bool, error) {
	meta, err := s.getCollectionMeta(collection, txn)
	if err == nil {
		for _, idx := range meta.Indexes {
			if idx == field {
				return true, nil
			}
		}
	}
	return false, err
}

func (s *storageImpl) HasIndex(collection, field string) (bool, error) {
	txn := s.db.NewTransaction(false)
	defer txn.Discard()

	return s.hasIndex(txn, collection, field)
}

func (s *storageImpl) getIndexes(collection string, meta *collectionMetadata) []*indexImpl {
	indexes := make([]*indexImpl, 0)

	for _, idxField := range meta.Indexes {
		indexes = append(indexes, &indexImpl{
			collectionName: collection,
			fieldName:      idxField,
		})
	}
	return indexes
}

func (s *storageImpl) listIndexes(collection string, txn *badger.Txn) ([]string, error) {
	meta, err := s.getCollectionMeta(collection, txn)
	return meta.Indexes, err
}

func (s *storageImpl) ListIndexes(collection string) ([]string, error) {
	txn := s.db.NewTransaction(false)
	defer txn.Discard()

	return s.listIndexes(collection, txn)
}
