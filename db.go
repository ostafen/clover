package clover

import (
	"errors"
	"fmt"

	uuid "github.com/satori/go.uuid"
)

// Collection creation errors
var (
	ErrCollectionExist    = errors.New("collection already exist")
	ErrCollectionNotExist = errors.New("no such collection")
	ErrIndexExist         = errors.New("index already exist")
	ErrIndexNotExist      = errors.New("no such index")
)

// DB represents the entry point of each clover database.
type DB struct {
	dir    string
	engine StorageEngine
}

// CreateCollection creates a new empty collection with the given name.
func (db *DB) CreateCollection(name string) error {
	return db.engine.CreateCollection(name)
}

// DropCollection removes the collection with the given name, deleting any content on disk.
func (db *DB) DropCollection(name string) error {
	return db.engine.DropCollection(name)
}

// HasCollection returns true if and only if the database contains a collection with the given name.
func (db *DB) HasCollection(name string) (bool, error) {
	return db.engine.HasCollection(name)
}

func NewObjectId() string {
	return uuid.NewV4().String()
}

func isValidObjectId(id string) bool {
	_, err := uuid.FromString(id)
	return err == nil
}

// Insert adds the supplied documents to a collection.
func (db *DB) Insert(collectionName string, docs ...*Document) error {
	for _, doc := range docs {
		if !doc.Has(objectIdField) {
			objectId := NewObjectId()
			doc.Set(objectIdField, objectId)
		}
	}
	return db.engine.Insert(collectionName, docs...)
}

// Save or update a document
func (db *DB) Save(collectionName string, doc *Document) error {
	if !doc.Has(objectIdField) {
		return db.Insert(collectionName, doc)
	}
	return db.ReplaceById(collectionName, doc.ObjectId(), doc)
}

// InsertOne inserts a single document to an existing collection. It returns the id of the inserted document.
func (db *DB) InsertOne(collectionName string, doc *Document) (string, error) {
	err := db.Insert(collectionName, doc)
	return doc.ObjectId(), err
}

// Open opens a new clover database on the supplied path. If such a folder doesn't exist, it is automatically created.
func Open(dir string, opts ...Option) (*DB, error) {
	config, err := defaultConfig().applyOptions(opts)
	if err != nil {
		return nil, err
	}

	db := &DB{
		dir:    dir,
		engine: newDefaultStorageImpl(),
	}
	return db, db.engine.Open(dir, config)
}

// Close releases all the resources and closes the database. After the call, the instance will no more be usable.
func (db *DB) Close() error {
	return db.engine.Close()
}

// FindAll selects all the documents satisfying q.
func (db *DB) FindAll(q *Query) ([]*Document, error) {
	if err := q.normalizeCriteria(); err != nil {
		return nil, err
	}
	return db.engine.FindAll(q)
}

// FindFirst returns the first document (if any) satisfying the query.
func (db *DB) FindFirst(q *Query) (*Document, error) {
	docs, err := db.FindAll(q.Limit(1))

	var doc *Document
	if len(docs) > 0 {
		doc = docs[0]
	}
	return doc, err
}

// ForEach runs the consumer function for each document matching the provied query.
// If false is returned from the consumer function, then the iteration is stopped.
func (db *DB) ForEach(q *Query, consumer func(_ *Document) bool) error {
	if err := q.normalizeCriteria(); err != nil {
		return err
	}

	return db.engine.IterateDocs(q, func(doc *Document) error {
		if !consumer(doc) {
			return errStopIteration
		}
		return nil
	})
}

// Count returns the number of documents which satisfy the query (i.e. len(q.FindAll()) == q.Count()).
func (db *DB) Count(q *Query) (int, error) {
	if err := q.normalizeCriteria(); err != nil {
		return -1, err
	}

	num, err := db.engine.Count(q)
	return num, err
}

// Exists returns true if and only if the query result set is not empty.
func (db *DB) Exists(q *Query) (bool, error) {
	doc, err := db.FindFirst(q)
	return doc != nil, err
}

// FindById returns the document with the given id, if such a document exists and satisfies the underlying query, or null.
func (db *DB) FindById(collection string, id string) (*Document, error) {
	return db.engine.FindById(collection, id)
}

// DeleteById removes the document with the given id from the underlying collection, provided that such a document exists and satisfies the underlying query.
func (db *DB) DeleteById(collection string, id string) error {
	return db.engine.DeleteById(collection, id)
}

// UpdateById updates the document with the specified id using the supplied update map.
// If no document with the specified id exists, an ErrDocumentNotExist is returned.
func (db *DB) UpdateById(collection, docId string, updateMap map[string]interface{}) error {
	return db.engine.UpdateById(collection, docId, func(doc *Document) *Document {
		newDoc := doc.Copy()
		newDoc.SetAll(updateMap)
		return newDoc
	})
}

// ReplaceById replaces the document with the specified id with the one provided.
// If no document exists, an ErrDocumentNotExist is returned.
func (db *DB) ReplaceById(collection, docId string, doc *Document) error {
	if doc.ObjectId() != docId {
		return fmt.Errorf("the id of the document must match the one supplied")
	}
	return db.engine.UpdateById(collection, docId, func(_ *Document) *Document {
		return doc
	})
}

// Update updates all the document selected by q using the provided updateMap.
// Each update is specified by a mapping fieldName -> newValue.
func (db *DB) Update(q *Query, updateMap map[string]interface{}) error {
	if err := q.normalizeCriteria(); err != nil {
		return err
	}

	return db.UpdateFunc(q, func(doc *Document) *Document {
		newDoc := doc.Copy()
		newDoc.SetAll(updateMap)
		return newDoc
	})
}

// Update updates all the document selected by q using the provided function.
func (db *DB) UpdateFunc(q *Query, updateFunc func(doc *Document) *Document) error {
	if err := q.normalizeCriteria(); err != nil {
		return err
	}
	return db.engine.Update(q.clearSortSkipAndLimit(), updateFunc)
}

// Delete removes all the documents selected by q from the underlying collection.
func (db *DB) Delete(q *Query) error {
	if err := q.normalizeCriteria(); err != nil {
		return err
	}
	return db.engine.Delete(q.clearSortSkipAndLimit())
}

// ListCollections returns a slice of strings containing the name of each collection stored in the db.
func (db *DB) ListCollections() ([]string, error) {
	return db.engine.ListCollections()
}

func (db *DB) CreateIndex(collection, field string) error {
	return db.engine.CreateIndex(collection, field)
}

func (db *DB) HasIndex(collection, field string) (bool, error) {
	return db.engine.HasIndex(collection, field)
}

func (db *DB) DropIndex(collection, field string) error {
	return db.engine.DropIndex(collection, field)
}

func (db *DB) ListIndexes(collection string) ([]string, error) {
	return db.engine.ListIndexes(collection)
}
