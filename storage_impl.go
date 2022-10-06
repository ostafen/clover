package clover

import (
	"bytes"
	"encoding/json"
	"sync/atomic"

	d "github.com/ostafen/clover/v2/document"
	"github.com/ostafen/clover/v2/index"
	"github.com/ostafen/clover/v2/internal"
	"github.com/ostafen/clover/v2/query"
	"github.com/ostafen/clover/v2/store"
)

type storageImpl struct {
	store  store.Store
	closed uint32
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
	tx, err := s.store.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	ok, err := s.hasCollection(name, tx)
	if err != nil {
		return err
	}

	if ok {
		return ErrCollectionExist
	}

	meta := &collectionMetadata{Size: 0}
	if err := s.saveCollectionMetadata(name, meta, tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *storageImpl) DropCollection(name string) error {
	tx, err := s.store.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := s.deleteAll(tx, name); err != nil {
		return err
	}

	if err := tx.Delete([]byte(getCollectionKey(name))); err != nil {
		return err
	}

	return tx.Commit()
}

func (s *storageImpl) getCollectionMeta(collection string, tx store.Tx) (*collectionMetadata, error) {
	value, err := tx.Get([]byte(getCollectionKey(collection)))
	if err != nil {
		return nil, err
	}

	if value == nil {
		return nil, ErrCollectionNotExist
	}

	m := &collectionMetadata{}
	err = json.Unmarshal(value, m)
	return m, err
}

func (s *storageImpl) saveCollectionMetadata(collection string, meta *collectionMetadata, tx store.Tx) error {
	rawMeta, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return tx.Set([]byte(getCollectionKey(collection)), rawMeta)
}

func (s *storageImpl) getCollectionSize(collection string) (int, error) {
	tx, err := s.store.Begin(false)
	if err != nil {
		return -1, err
	}
	defer tx.Rollback()

	meta, err := s.getCollectionMeta(collection, tx)
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

func getDocumentById(collectionName string, id string, tx store.Tx) (*d.Document, error) {
	value, err := tx.Get([]byte(getDocumentKey(collectionName, id)))
	if value == nil || err != nil {
		return nil, err
	}
	return d.Decode(value)
}

func (s *storageImpl) FindById(collectionName string, id string) (*d.Document, error) {
	tx, err := s.store.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	ok, err := s.hasCollection(collectionName, tx)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, ErrCollectionNotExist
	}

	return getDocumentById(collectionName, id, tx)
}

func getDocumentKeyPrefix(collection string) string {
	return "c:" + collection + ";" + "d:"
}

func getDocumentKey(collection string, id string) string {
	return getDocumentKeyPrefix(collection) + id
}

func (s *storageImpl) addDocToIndexes(tx store.Tx, indexes []index.Index, doc *d.Document) error {
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
	tx, err := s.store.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	meta, err := s.getCollectionMeta(collection, tx)
	if err != nil {
		return err
	}

	indexes := s.getIndexes(tx, collection, meta)

	for _, doc := range docs {
		if err := s.addDocToIndexes(tx, indexes, doc); err != nil {
			return err
		}

		key := []byte(getDocumentKey(collection, doc.ObjectId()))
		value, err := tx.Get(key)
		if err != nil {
			return err
		}

		if value != nil {
			return ErrDuplicateKey
		}

		if err := saveDocument(doc, key, tx); err != nil {
			return err
		}
	}

	meta.Size += len(docs)
	if err := s.saveCollectionMetadata(collection, meta, tx); err != nil {
		return err
	}

	return tx.Commit()
}

func saveDocument(doc *d.Document, key []byte, tx store.Tx) error {
	if err := d.Validate(doc); err != nil {
		return err
	}

	data, err := d.Encode(doc)
	if err != nil {
		return err
	}
	return tx.Set(key, data)
}

func (s *storageImpl) deleteDocFromIndexes(indexes []index.Index, doc *d.Document) error {
	for _, idx := range indexes {
		value := doc.Get(idx.Field())
		if err := idx.Remove(doc.ObjectId(), value); err != nil {
			return err
		}
	}
	return nil
}

func (s *storageImpl) getDocAndDeleteFromIndexes(tx store.Tx, indexes []index.Index, collection string, docId string) error {
	if len(indexes) == 0 {
		return nil
	}

	doc, err := getDocumentById(collection, docId, tx)
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

func (s *storageImpl) updateIndexesOnDocUpdate(tx store.Tx, indexes []index.Index, oldDoc, newDoc *d.Document) error {
	if err := s.deleteDocFromIndexes(indexes, oldDoc); err != nil {
		return err
	}

	if newDoc != nil {
		if err := s.addDocToIndexes(tx, indexes, newDoc); err != nil {
			return err
		}
	}

	return nil
}

type docUpdater func(doc *d.Document) *d.Document

func (s *storageImpl) replaceDocs(tx store.Tx, q *query.Query, updater docUpdater) error {
	meta, err := s.getCollectionMeta(q.Collection(), tx)
	if err != nil {
		return err
	}

	indexes := s.getIndexes(tx, q.Collection(), meta)

	deletedDocs := 0
	err = s.iterateDocs(tx, q, func(doc *d.Document) error {
		docKey := []byte(getDocumentKey(q.Collection(), doc.ObjectId()))
		newDoc := updater(doc)

		if err := s.updateIndexesOnDocUpdate(tx, indexes, doc, newDoc); err != nil {
			return err
		}

		if newDoc == nil {
			deletedDocs++
			return tx.Delete(docKey)
		}

		return saveDocument(newDoc, docKey, tx)
	})

	if err != nil {
		return err
	}

	if deletedDocs > 0 {
		meta.Size -= deletedDocs
		if err := s.saveCollectionMetadata(q.Collection(), meta, tx); err != nil {
			return err
		}
	}
	return nil
}

func (s *storageImpl) Update(q *query.Query, updater func(doc *d.Document) *d.Document) error {
	txn, err := s.store.Begin(true)
	if err != nil {
		return err
	}
	defer txn.Rollback()

	if err := s.replaceDocs(txn, q, updater); err != nil {
		return err
	}
	return txn.Commit()
}

func (s *storageImpl) deleteAll(tx store.Tx, collName string) error {
	return s.replaceDocs(tx, query.NewQuery(collName), func(_ *d.Document) *d.Document {
		return nil
	})
}

func (s *storageImpl) Delete(q *query.Query) error {
	tx, err := s.store.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := s.replaceDocs(tx, q, func(_ *d.Document) *d.Document { return nil }); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *storageImpl) DeleteById(collName string, id string) error {
	tx, err := s.store.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	meta, err := s.getCollectionMeta(collName, tx)
	if err != nil {
		return err
	}

	indexes := s.getIndexes(tx, collName, meta)

	if err := s.getDocAndDeleteFromIndexes(tx, indexes, collName, id); err != nil {
		return err
	}

	if err := tx.Delete([]byte(getDocumentKey(collName, id))); err != nil {
		return err
	}

	meta.Size--
	if err := s.saveCollectionMetadata(collName, meta, tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *storageImpl) UpdateById(collectionName string, docId string, updater func(doc *d.Document) *d.Document) error {
	tx, err := s.store.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	meta, err := s.getCollectionMeta(collectionName, tx)
	if err != nil {
		return err
	}

	indexes := s.getIndexes(tx, collectionName, meta)

	docKey := getDocumentKey(collectionName, docId)
	value, err := tx.Get([]byte(docKey))
	if err != nil {
		return err
	}

	if value == nil {
		return ErrDocumentNotExist
	}

	doc, err := d.Decode(value)
	if err != nil {
		return err
	}

	updatedDoc := updater(doc)
	if err := s.updateIndexesOnDocUpdate(tx, indexes, doc, updatedDoc); err != nil {
		return err
	}

	if err := saveDocument(updatedDoc, []byte(docKey), tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *storageImpl) Close() error {
	if atomic.CompareAndSwapUint32(&s.closed, 0, 1) {
		return s.store.Close()
	}
	return nil
}

func (s *storageImpl) hasCollection(name string, tx store.Tx) (bool, error) {
	value, err := tx.Get([]byte(getCollectionKey(name)))
	return value != nil, err
}

func (s *storageImpl) HasCollection(name string) (bool, error) {
	txn, err := s.store.Begin(false)
	if err != nil {
		return false, err
	}
	defer txn.Rollback()
	return s.hasCollection(name, txn)
}

func iteratePrefix(prefix []byte, tx store.Tx, itemConsumer func(item store.Item) error) error {
	cursor, err := tx.Cursor(true)
	if err != nil {
		return err
	}
	defer cursor.Close()

	if err := cursor.Seek(prefix); err != nil {
		return err
	}

	if err := cursor.Seek(prefix); err != nil {
		return err
	}

	for ; cursor.Valid(); cursor.Next() {
		item, err := cursor.Item()
		if err != nil {
			return err
		}

		if !bytes.HasPrefix(item.Key, prefix) {
			return nil
		}
		err = itemConsumer(item)

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

func (s *storageImpl) iterateDocs(tx store.Tx, q *query.Query, consumer docConsumer) error {
	meta, err := s.getCollectionMeta(q.Collection(), tx)
	if err != nil {
		return err
	}
	nd := buildQueryPlan(q, s.getIndexes(tx, q.Collection(), meta), &consumerNode{consumer: consumer})
	return execPlan(nd, tx)
}

func (s *storageImpl) IterateDocs(q *query.Query, consumer docConsumer) error {
	tx, err := s.store.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return s.iterateDocs(tx, q, consumer)
}

func (s *storageImpl) ListCollections() ([]string, error) {
	tx, err := s.store.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	collections := make([]string, 0)

	prefix := []byte(getCollectionKeyPrefix())
	err = iteratePrefix(prefix, tx, func(item store.Item) error {
		collectionName := string(bytes.TrimPrefix(item.Key, prefix))
		collections = append(collections, collectionName)
		return nil
	})
	return collections, err
}

func (s *storageImpl) createIndex(collection, field string, indexType index.IndexType) error {
	tx, err := s.store.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	meta, err := s.getCollectionMeta(collection, tx)
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

	idx := index.CreateIndex(collection, field, indexType, tx)

	err = s.iterateDocs(tx, query.NewQuery(collection), func(doc *d.Document) error {
		value := doc.Get(field)
		return idx.Add(doc.ObjectId(), value, doc.TTL())
	})

	if err != nil {
		return err
	}

	if err := s.saveCollectionMetadata(collection, meta, tx); err != nil {
		return err
	}

	return tx.Commit()
}

func (s *storageImpl) CreateIndex(collection, field string) error {
	return s.createIndex(collection, field, index.IndexSingleField)
}

func (s *storageImpl) DropIndex(collection, field string) error {
	txn, err := s.store.Begin(true)
	if err != nil {
		return err
	}
	defer txn.Rollback()

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

	idx := index.CreateIndex(collection, field, idxType, txn)

	if err := idx.Drop(); err != nil {
		return err
	}

	if err := s.saveCollectionMetadata(collection, meta, txn); err != nil {
		return err
	}
	return txn.Commit()
}

func (s *storageImpl) hasIndex(tx store.Tx, collection, field string) (bool, error) {
	meta, err := s.getCollectionMeta(collection, tx)
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
	tx, err := s.store.Begin(false)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	return s.hasIndex(tx, collection, field)
}

func (s *storageImpl) getIndexes(tx store.Tx, collection string, meta *collectionMetadata) []index.Index {
	indexes := make([]index.Index, 0)

	for _, info := range meta.Indexes {
		indexes = append(indexes, index.CreateIndex(collection, info.Field, info.Type, tx))
	}
	return indexes
}

func (s *storageImpl) listIndexes(collection string, tx store.Tx) ([]index.IndexInfo, error) {
	meta, err := s.getCollectionMeta(collection, tx)
	return meta.Indexes, err
}

func (s *storageImpl) ListIndexes(collection string) ([]index.IndexInfo, error) {
	txn, err := s.store.Begin(false)
	if err != nil {
		return nil, err
	}
	defer txn.Rollback()

	return s.listIndexes(collection, txn)
}
