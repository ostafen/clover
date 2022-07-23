package clover

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	badger "github.com/dgraph-io/badger/v3"
	"github.com/ostafen/clover/internal"
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
	Size int
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

func (s *storageImpl) getDocumentById(collectionName string, id string, txn *badger.Txn) (*Document, error) {
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

	return s.getDocumentById(collectionName, id, txn)
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

	indexes, err := s.listIndexes(collection, txn)

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

func (s *storageImpl) getDocAndDeleteFromIndexes(txn *badger.Txn, collection string, docId string) error {
	indexes, err := s.listIndexes(collection, txn)
	if err != nil {
		return err
	}

	if len(indexes) == 0 {
		return nil
	}

	doc, err := s.getDocumentById(collection, docId, txn)
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

	indexes, err := s.listIndexes(q.collection, txn)
	if err != nil {
		return err
	}

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

	if err := s.getDocAndDeleteFromIndexes(txn, collName, id); err != nil {
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
		has, err := s.hasCollection(collectionName, txn)
		if err != nil {
			return err
		}

		if !has {
			return ErrCollectionNotExist
		}

		indexes, err := s.listIndexes(collectionName, txn)
		if err != nil {
			return err
		}

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

func (s *storageImpl) tryIterateDocsFromIndex(q *Query, txn *badger.Txn, consumer docConsumer) error {
	if q.criteria == nil {
		return errNoSuitableIndexFound
	}

	indexQueries, err := s.getQueryIndexes(q, txn)
	if err != nil {
		return err
	}

	if len(indexQueries) != 1 {
		return errNoSuitableIndexFound
	}

	indexQuery := indexQueries[0]
	needSort := len(q.sortOpts) > 0

	reversed := false
	if len(q.sortOpts) == 1 && q.sortOpts[0].Field == indexQuery.index.fieldName {
		needSort = false
		reversed = q.sortOpts[0].Direction < 0
	}

	var docs []*Document
	if needSort {
		docs = make([]*Document, 0)
	} else {
		consumer = withSkipAndLimitConsumer(q, consumer)
	}

	err = indexQuery.index.IterateRange(txn, indexQuery.vRange, reversed, func(docId string) error {
		doc, err := s.getDocumentById(q.collection, docId, txn)

		// err == badger.ErrKeyNotFound when index record expires before document record
		if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		if !q.satisfy(doc) {
			return nil
		}

		if !needSort {
			return consumer(doc)
		}

		docs = append(docs, doc)
		return nil
	})

	if err == nil && needSort {
		return s.applySortSkipAndLimit(docs, q, consumer)
	}
	return err
}

func withSkipAndLimitConsumer(q *Query, consumer docConsumer) docConsumer {
	skipped := 0
	consumed := 0
	return func(doc *Document) error {
		if skipped < q.skip {
			skipped++
			return nil
		}

		if q.limit >= 0 && consumed >= q.limit {
			return errStopIteration
		}

		if err := consumer(doc); err != nil {
			return err
		}
		consumed++

		if q.limit >= 0 && consumed >= q.limit {
			return errStopIteration
		}
		return nil
	}
}

func (s *storageImpl) iterateDocs(txn *badger.Txn, q *Query, consumer docConsumer) error {
	if txn == nil {
		txn = s.db.NewTransaction(false)
		defer txn.Discard()
	}

	ok, err := s.hasCollection(q.collection, txn)
	if err != nil {
		return err
	}

	if !ok {
		return ErrCollectionNotExist
	}

	err = s.tryIterateDocsFromIndex(q, txn, consumer)
	if errors.Is(err, errNoSuitableIndexFound) {
		return s.iterateCollection(q, txn, consumer)
	}
	return err
}

func (s *storageImpl) iterateCollection(q *Query, txn *badger.Txn, consumer docConsumer) error {
	prefix := []byte(getDocumentKeyPrefix(q.collection))

	var docs []*Document

	needSort := len(q.sortOpts) > 0
	if needSort {
		docs = make([]*Document, 0)
	} else {
		consumer = withSkipAndLimitConsumer(q, consumer)
	}

	err := iteratePrefix(prefix, txn, func(item *badger.Item) error {
		return item.Value(func(data []byte) error {
			doc, err := decodeDoc(data)
			if err != nil {
				return err
			}

			if !q.satisfy(doc) {
				return nil
			}

			if !needSort {
				return consumer(doc)
			}

			docs = append(docs, doc)
			return nil
		})
	})

	if err == nil && needSort {
		return s.applySortSkipAndLimit(docs, q, consumer)
	}
	return err
}

func (s *storageImpl) applySortSkipAndLimit(docs []*Document, q *Query, consumer docConsumer) error {
	sort.Slice(docs, func(i, j int) bool {
		return compareDocuments(docs[i], docs[j], q.sortOpts) < 0
	})

	docs = s.applySkipAndLimit(q, docs)

	for _, doc := range docs {
		err := consumer(doc)
		if err == errStopIteration {
			return nil
		}

		if err != nil {
			return err
		}
	}
	return nil
}

func (*storageImpl) applySkipAndLimit(q *Query, allDocs []*Document) []*Document {
	docsToSkip := q.skip
	if len(allDocs) < q.skip {
		docsToSkip = len(allDocs)
	}
	allDocs = allDocs[docsToSkip:]

	if q.limit >= 0 && len(allDocs) > q.limit {
		allDocs = allDocs[:q.limit]
	}
	return allDocs
}

func (s *storageImpl) getQueryIndexes(q *Query, txn *badger.Txn) ([]*indexQuery, error) {
	indexes, err := s.listIndexes(q.collection, txn)
	if err != nil {
		return nil, err
	}

	if len(indexes) == 0 {
		return nil, nil
	}

	indexedFields := make(map[string]bool)
	for _, idx := range indexes {
		indexedFields[idx.fieldName] = true
	}

	c := q.criteria.Accept(&NotFlattenVisitor{}).(Criteria)
	selectedFields := c.Accept(&IndexSelectVisitor{
		Fields: indexedFields,
	}).([]string)

	if len(selectedFields) == 0 {
		return nil, nil
	}

	fieldRanges := c.Accept(NewFieldRangeVisitor(selectedFields)).(map[string]*valueRange)

	indexesMap := make(map[string]*indexImpl)
	for _, idx := range indexes {
		indexesMap[idx.fieldName] = idx
	}

	queries := make([]*indexQuery, 0)
	for field, vRange := range fieldRanges {
		queries = append(queries, &indexQuery{
			vRange: vRange,
			index:  indexesMap[field],
		})
	}

	return queries, nil
}

func (s *storageImpl) IterateDocs(q *Query, consumer docConsumer) error {
	return s.iterateDocs(nil, q, consumer)
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

func getIndexKeyPrefix(collection string) []byte {
	return []byte("idx:" + collection + ":")
}

func getIndexKey(collection, field string) []byte {
	return append(getIndexKeyPrefix(collection), []byte(field)...)
}

func (s *storageImpl) getIndex(collection, field string, txn *badger.Txn) (*indexImpl, error) {
	_, err := txn.Get(getIndexKey(collection, field))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return &indexImpl{
		collectionName: collection,
		fieldName:      field,
	}, nil
}

func (s *storageImpl) CreateIndex(collection, field string) error {
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	ok, err := s.hasCollection(collection, txn)
	if err != nil {
		return err
	}

	if !ok {
		return ErrCollectionNotExist
	}

	has, err := s.hasIndex(txn, collection, field)
	if err != nil {
		return err
	}

	if has {
		return ErrIndexExist
	}

	if err := txn.Set(getIndexKey(collection, field), []byte{0}); err != nil {
		return err
	}

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

	return txn.Commit()
}

func (s *storageImpl) DropIndex(collection, field string) error {
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	ok, err := s.hasCollection(collection, txn)
	if err != nil {
		return err
	}

	if !ok {
		return ErrCollectionNotExist
	}

	idx, err := s.getIndex(collection, field, txn)
	if err != nil {
		return err
	}

	if idx == nil {
		return ErrIndexNotExist
	}

	if err := txn.Delete(getIndexKey(collection, field)); err != nil {
		return err
	}

	idx = &indexImpl{
		collectionName: collection,
		fieldName:      field,
	}

	if err := idx.deleteAll(txn); err != nil {
		return err
	}

	return txn.Commit()
}

func (s *storageImpl) hasIndex(txn *badger.Txn, collection, field string) (bool, error) {
	idx, err := s.getIndex(collection, field, txn)
	return idx != nil, err
}

func (s *storageImpl) HasIndex(collection, field string) (bool, error) {
	txn := s.db.NewTransaction(false)
	defer txn.Discard()

	return s.hasIndex(txn, collection, field)
}

func (s *storageImpl) listIndexes(collection string, txn *badger.Txn) ([]*indexImpl, error) {
	indexes := make([]*indexImpl, 0)

	prefix := getIndexKeyPrefix(collection)
	err := iteratePrefix(prefix, txn, func(item *badger.Item) error {
		key := string(item.Key())
		fieldName := strings.TrimPrefix(key, string(prefix))
		indexes = append(indexes, &indexImpl{collectionName: collection, fieldName: fieldName})
		return nil
	})
	return indexes, err
}

func (s *storageImpl) ListIndexes(collection string) ([]string, error) {
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	indexes, err := s.listIndexes(collection, txn)
	if err != nil {
		return nil, err
	}

	fields := make([]string, 0)
	for _, idx := range indexes {
		fields = append(fields, idx.fieldName)
	}
	return fields, nil
}
