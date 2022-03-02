package clover

import (
	"encoding/json"
	"errors"

	badger "github.com/dgraph-io/badger/v3"
)

type docConsumer func(doc *Document) error

// StorageEngine represents the persistance layer and abstracts how collections are stored.
type StorageEngine interface {
	Open(path string) error
	Close() error

	CreateCollection(name string) error
	DropCollection(name string) error
	HasCollection(name string) (bool, error)
	FindAll(q *Query) ([]*Document, error)
	FindById(collectionName string, id string) (*Document, error)
	DeleteById(collectionName string, id string) error
	IterateDocs(collectionName string, consumer docConsumer) error
	Insert(collection string, docs ...*Document) error
	Update(q *Query, updateMap map[string]interface{}) error
	Delete(q *Query) error
}

type storageImpl struct {
	db *badger.DB
}

func newStorageImpl() *storageImpl {
	return &storageImpl{}
}

func (s *storageImpl) Open(path string) error {
	db, err := badger.Open(badger.DefaultOptions(path).WithLoggingLevel(badger.ERROR))
	s.db = db
	return err
}

func getCollectionKey(name string) string {
	return "coll:" + name
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

	if err := txn.Set([]byte(getCollectionKey(name)), []byte{0}); err != nil {
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

func (s *storageImpl) FindAll(q *Query) ([]*Document, error) {
	docs := make([]*Document, 0)
	err := s.IterateDocs(q.collection, func(doc *Document) error {
		if q.satisfy(doc) {
			docs = append(docs, doc)
		}
		return nil
	})
	return docs, err
}

func readDoc(data []byte) (*Document, error) {
	doc := NewDocument()
	return doc, json.Unmarshal(data, &doc.fields)
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

	item, err := txn.Get([]byte(getDocumentKey(collectionName, id)))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}

	var doc *Document
	err = item.Value(func(data []byte) error {
		d, err := readDoc(data)
		doc = d
		return err
	})
	return doc, err
}

func getDocumentKey(collection string, id string) string {
	return collection + ":" + id
}

func (s *storageImpl) Insert(collection string, docs ...*Document) error {
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	ok, err := s.hasCollection(collection, txn)
	if err != nil {
		return err
	}

	if !ok {
		return ErrCollectionNotExist
	}

	for _, doc := range docs {
		data, err := json.Marshal(doc.fields)
		if err != nil {
			return err
		}

		if err := txn.Set([]byte(getDocumentKey(collection, doc.ObjectId())), data); err != nil {
			return err
		}
	}
	return txn.Commit()
}

type docUpdater func(doc *Document) *Document

func (s *storageImpl) replaceDocs(txn *badger.Txn, q *Query, updater docUpdater) error {
	if txn == nil {
		txn = s.db.NewTransaction(true)
		defer txn.Discard()
	}

	ok, err := s.hasCollection(q.collection, txn)
	if err != nil {
		return err
	}

	if !ok {
		return ErrCollectionNotExist
	}

	docs := make([]*Document, 0)
	s.iterateDocs(txn, q.collection, func(doc *Document) error {
		if q.satisfy(doc) {
			docs = append(docs, doc)
		}
		return nil
	})

	for _, doc := range docs {
		key := []byte(getDocumentKey(q.collection, doc.ObjectId()))
		if q.satisfy(doc) {
			newDoc := updater(doc)
			if newDoc == nil {
				if err := txn.Delete(key); err != nil {
					return err
				}
			} else {
				data, err := json.Marshal(newDoc.fields)
				if err != nil {
					return err
				}
				if err := txn.Set([]byte(getDocumentKey(q.collection, doc.ObjectId())), data); err != nil {
					return err
				}
			}
		}
	}
	return txn.Commit()
}

func (s *storageImpl) Update(q *Query, updateMap map[string]interface{}) error {
	return s.replaceDocs(nil, q, func(doc *Document) *Document {
		updateDoc := doc.Copy()
		for updateField, updateValue := range updateMap {
			updateDoc.Set(updateField, updateValue)
		}
		return updateDoc
	})
}

func (s *storageImpl) deleteAll(txn *badger.Txn, collName string) error {
	return s.replaceDocs(txn, &Query{collection: collName}, func(_ *Document) *Document {
		return nil
	})
}

func (s *storageImpl) Delete(q *Query) error {
	return s.replaceDocs(nil, q, func(_ *Document) *Document {
		return nil
	})
}

func (s *storageImpl) DeleteById(collName string, id string) error {
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	ok, err := s.hasCollection(collName, txn)
	if err != nil {
		return err
	}

	if !ok {
		return ErrCollectionNotExist
	}

	if err := txn.Delete([]byte(getDocumentKey(collName, id))); err != nil {
		return err
	}
	return txn.Commit()
}

func (s *storageImpl) Close() error {
	return s.db.Close()
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

func (s *storageImpl) iterateDocs(txn *badger.Txn, collName string, consumer docConsumer) error {
	if txn == nil {
		txn = s.db.NewTransaction(false)
		defer txn.Discard()
	}

	ok, err := s.hasCollection(collName, txn)
	if err != nil {
		return err
	}

	if !ok {
		return ErrCollectionNotExist
	}

	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	prefix := []byte(collName + ":")
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		err := it.Item().Value(func(data []byte) error {
			doc, err := readDoc(data)
			if err != nil {
				return err
			}
			return consumer(doc)
		})

		if err != nil {
			return err
		}
	}
	return nil
}

func (s *storageImpl) IterateDocs(collName string, consumer docConsumer) error {
	txn := s.db.NewTransaction(false)
	defer txn.Discard()
	return s.iterateDocs(nil, collName, consumer)
}
