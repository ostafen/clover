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
	"github.com/mmcloughlin/geohash"
	d "github.com/ostafen/clover/v2/document"
	"github.com/ostafen/clover/v2/index"
	"github.com/ostafen/clover/v2/internal"
	"github.com/ostafen/clover/v2/query"
	"github.com/ostafen/clover/v2/util"
)

type storageImpl struct {
	db     *badger.DB
	conf   *Config
	chQuit chan struct{}
	chWg   sync.WaitGroup
	closed uint32
}

func NewDefaultStorage() *storageImpl {
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
	Indexes []index.IndexInfo
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

func (s *storageImpl) countCollection(q *query.Query) (int, error) {
	size, err := s.getCollectionSize(q.Collection())
	size -= q.GetSkip()

	if size < 0 {
		size = 0
	}

	if q.GetLimit() >= 0 && q.GetLimit() < size {
		return q.GetLimit(), err
	}

	return size, err
}

func (s *storageImpl) Count(q *query.Query) (int, error) {
	if q.Criteria() == nil { // simply return the size of the collection in this case
		return s.countCollection(q)
	}

	num := 0
	err := s.IterateDocs(q, func(doc *d.Document) error {
		num++
		return nil
	})
	return num, err
}

func (s *storageImpl) FindAll(q *query.Query) ([]*d.Document, error) {
	docs := make([]*d.Document, 0)

	err := s.IterateDocs(q, func(doc *d.Document) error {
		docs = append(docs, doc)
		return nil
	})
	return docs, err
}

func getDocumentById(collectionName string, id string, txn *badger.Txn) (*d.Document, error) {
	item, err := txn.Get([]byte(getDocumentKey(collectionName, id)))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}

	var doc *d.Document
	err = item.Value(func(data []byte) error {
		d, err := d.Decode(data)
		doc = d
		return err
	})
	return doc, err
}

func (s *storageImpl) FindById(collectionName string, id string) (*d.Document, error) {
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

func getGeoHash(v interface{}) int64 {
	s := v.([]interface{})
	if !util.IsNumber(s[0]) && !util.IsNumber(s[1]) {
		return -1
	}

	x := util.ToFloat64(s[0])
	y := util.ToFloat64(s[1])
	return int64(geohash.EncodeIntWithPrecision(x, y, 26))
}

func (s *storageImpl) addDocToIndexes(txn *badger.Txn, indexes []index.Index, doc *d.Document) error {
	// update indexes
	for _, idx := range indexes {
		fieldVal := doc.Get(idx.Field()) // missing fields are treated as null

		err := idx.Add(doc.ObjectId(), fieldVal, doc.TTL())
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *storageImpl) Insert(collection string, docs ...*d.Document) error {
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	meta, err := s.getCollectionMeta(collection, txn)
	if err != nil {
		return err
	}

	indexes := s.getIndexes(txn, collection, meta)

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

func saveDocument(doc *d.Document, key []byte, txn *badger.Txn) error {
	if err := d.Validate(doc); err != nil {
		return err
	}

	data, err := d.Encode(doc)
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

func (s *storageImpl) deleteDocFromIndexes(txn *badger.Txn, indexes []index.Index, doc *d.Document) error {
	for _, idx := range indexes {
		value := doc.Get(idx.Field())
		if err := idx.Remove(doc.ObjectId(), value); err != nil {
			return err
		}
	}
	return nil
}

func (s *storageImpl) getDocAndDeleteFromIndexes(txn *badger.Txn, indexes []index.Index, collection string, docId string) error {
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
		value := doc.Get(idx.Field())
		if err := idx.Remove(doc.ObjectId(), value); err != nil {
			return err
		}
	}
	return nil
}

func (s *storageImpl) updateIndexesOnDocUpdate(txn *badger.Txn, indexes []index.Index, oldDoc, newDoc *d.Document) error {
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

type docUpdater func(doc *d.Document) *d.Document

func (s *storageImpl) replaceDocs(txn *badger.Txn, q *query.Query, updater docUpdater) error {
	meta, err := s.getCollectionMeta(q.Collection(), txn)
	if err != nil {
		return err
	}

	indexes := s.getIndexes(txn, q.Collection(), meta)

	deletedDocs := 0
	err = s.iterateDocs(txn, q, func(doc *d.Document) error {
		docKey := []byte(getDocumentKey(q.Collection(), doc.ObjectId()))
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
		if err := s.saveCollectionMetadata(q.Collection(), meta, txn); err != nil {
			return err
		}
	}
	return nil
}

func (s *storageImpl) Update(q *query.Query, updater func(doc *d.Document) *d.Document) error {
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	if err := s.replaceDocs(txn, q, updater); err != nil {
		return err
	}
	return txn.Commit()
}

func (s *storageImpl) deleteAll(txn *badger.Txn, collName string) error {
	return s.replaceDocs(txn, query.NewQuery(collName), func(_ *d.Document) *d.Document {
		return nil
	})
}

func (s *storageImpl) Delete(q *query.Query) error {
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	if err := s.replaceDocs(txn, q, func(_ *d.Document) *d.Document { return nil }); err != nil {
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

	indexes := s.getIndexes(txn, collName, meta)

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

func (s *storageImpl) UpdateById(collectionName string, docId string, updater func(doc *d.Document) *d.Document) error {
	return s.db.Update(func(txn *badger.Txn) error {
		meta, err := s.getCollectionMeta(collectionName, txn)
		if err != nil {
			return err
		}

		indexes := s.getIndexes(txn, collectionName, meta)

		docKey := getDocumentKey(collectionName, docId)
		item, err := txn.Get([]byte(docKey))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrDocumentNotExist
		}

		var doc *d.Document
		err = item.Value(func(value []byte) error {
			d, err := d.Decode(value)
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
		if err == internal.ErrStopIteration {
			return nil
		}

		if err != nil {
			return err
		}
	}
	return nil
}

var errNoSuitableIndexFound = errors.New("no suitable index found for running provided query")

func (s *storageImpl) iterateDocs(txn *badger.Txn, q *query.Query, consumer docConsumer) error {
	meta, err := s.getCollectionMeta(q.Collection(), txn)
	if err != nil {
		return err
	}
	nd := buildQueryPlan(q, s.getIndexes(txn, q.Collection(), meta), &consumerNode{consumer: consumer})
	return execPlan(nd, txn)
}

func (s *storageImpl) IterateDocs(q *query.Query, consumer docConsumer) error {
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

func (s *storageImpl) createIndex(collection, field string, indexType index.IndexType) error {
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	meta, err := s.getCollectionMeta(collection, txn)
	if err != nil {
		return err
	}

	for i := 0; i < len(meta.Indexes); i++ {
		if meta.Indexes[i].Field == field {
			return ErrIndexExist
		}
	}

	if meta.Indexes == nil {
		meta.Indexes = make([]index.IndexInfo, 0)
	}
	meta.Indexes = append(meta.Indexes, index.IndexInfo{Field: field, Type: indexType})

	idx := index.CreateBadgerIndex(collection, field, indexType, txn)

	err = s.iterateDocs(txn, query.NewQuery(collection), func(doc *d.Document) error {
		value := doc.Get(field)
		return idx.Add(doc.ObjectId(), value, doc.TTL())
	})

	if err != nil {
		return err
	}

	if err := s.saveCollectionMetadata(collection, meta, txn); err != nil {
		return err
	}

	return txn.Commit()
}

func (s *storageImpl) CreateIndex(collection, field string) error {
	return s.createIndex(collection, field, index.IndexSingleField)
}

func (s *storageImpl) DropIndex(collection, field string) error {
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	meta, err := s.getCollectionMeta(collection, txn)
	if err != nil {
		return err
	}

	j := -1
	for i := 0; i < len(meta.Indexes); i++ {
		if meta.Indexes[i].Field == field {
			j = i
		}
	}

	if j < 0 {
		return ErrIndexNotExist
	}

	idxType := meta.Indexes[j].Type

	meta.Indexes[j] = meta.Indexes[0]
	meta.Indexes = meta.Indexes[1:]

	idx := index.CreateBadgerIndex(collection, field, idxType, txn)

	if err := idx.Drop(); err != nil {
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
			if idx.Field == field {
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

func (s *storageImpl) getIndexes(txn *badger.Txn, collection string, meta *collectionMetadata) []index.Index {
	indexes := make([]index.Index, 0)

	for _, info := range meta.Indexes {
		indexes = append(indexes, index.CreateBadgerIndex(collection, info.Field, info.Type, txn))
	}
	return indexes
}

func (s *storageImpl) listIndexes(collection string, txn *badger.Txn) ([]index.IndexInfo, error) {
	meta, err := s.getCollectionMeta(collection, txn)
	return meta.Indexes, err
}

func (s *storageImpl) ListIndexes(collection string) ([]index.IndexInfo, error) {
	txn := s.db.NewTransaction(false)
	defer txn.Discard()

	return s.listIndexes(collection, txn)
}
