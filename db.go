package clover

import (
	"errors"
	"fmt"

	d "github.com/ostafen/clover/v2/document"
	"github.com/ostafen/clover/v2/index"
	"github.com/ostafen/clover/v2/internal"
	"github.com/ostafen/clover/v2/query"
	"github.com/ostafen/clover/v2/store"
	"github.com/ostafen/clover/v2/store/bbolt"
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

// Insert adds the supplied documents to a collection.
func (db *DB) Insert(collectionName string, docs ...*d.Document) error {
	for _, doc := range docs {
		if !doc.Has(d.ObjectIdField) {
			objectId := NewObjectId()
			doc.Set(d.ObjectIdField, objectId)
		}
	}
	return db.engine.Insert(collectionName, docs...)
}

// Save or update a document
func (db *DB) Save(collectionName string, doc *d.Document) error {
	if !doc.Has(d.ObjectIdField) {
		return db.Insert(collectionName, doc)
	}
	return db.ReplaceById(collectionName, doc.ObjectId(), doc)
}

// InsertOne inserts a single document to an existing collection. It returns the id of the inserted document.
func (db *DB) InsertOne(collectionName string, doc *d.Document) (string, error) {
	err := db.Insert(collectionName, doc)
	return doc.ObjectId(), err
}

// Open opens a new clover database on the supplied path. If such a folder doesn't exist, it is automatically created.
func Open(dir string, opts ...Option) (*DB, error) {
	config, err := defaultConfig().applyOptions(opts)
	if err != nil {
		return nil, err
	}

	store, err := getStoreOrOpenDefault(dir, config)
	if err != nil {
		return nil, err
	}

	db := &DB{
		dir: dir,
		engine: &storageImpl{
			store: store,
		},
	}
	return db, nil
}

func getStoreOrOpenDefault(path string, c *Config) (store.Store, error) {
	if c.store == nil {
		return openDefaultStore(path)
	}
	return c.store, nil
}

func openDefaultStore(dir string) (store.Store, error) {
	return bbolt.Open(dir)
}

// Close releases all the resources and closes the database. After the call, the instance will no more be usable.
func (db *DB) Close() error {
	return db.engine.Close()
}

// FindAll selects all the documents satisfying q.
func (db *DB) FindAll(q *query.Query) ([]*d.Document, error) {
	q, err := normalizeCriteria(q)
	if err != nil {
		return nil, err
	}
	return db.engine.FindAll(q)
}

// FindFirst returns the first document (if any) satisfying the query.
func (db *DB) FindFirst(q *query.Query) (*d.Document, error) {
	docs, err := db.FindAll(q.Limit(1))

	var doc *d.Document
	if len(docs) > 0 {
		doc = docs[0]
	}
	return doc, err
}

// ForEach runs the consumer function for each document matching the provied query.
// If false is returned from the consumer function, then the iteration is stopped.
func (db *DB) ForEach(q *query.Query, consumer func(_ *d.Document) bool) error {
	q, err := normalizeCriteria(q)
	if err != nil {
		return err
	}

	return db.engine.IterateDocs(q, func(doc *d.Document) error {
		if !consumer(doc) {
			return internal.ErrStopIteration
		}
		return nil
	})
}

// Count returns the number of documents which satisfy the query (i.e. len(q.FindAll()) == q.Count()).
func (db *DB) Count(q *query.Query) (int, error) {
	q, err := normalizeCriteria(q)
	if err != nil {
		return -1, err
	}

	num, err := db.engine.Count(q)
	return num, err
}

// Exists returns true if and only if the query result set is not empty.
func (db *DB) Exists(q *query.Query) (bool, error) {
	doc, err := db.FindFirst(q)
	return doc != nil, err
}

// FindById returns the document with the given id, if such a document exists and satisfies the underlying query, or null.
func (db *DB) FindById(collection string, id string) (*d.Document, error) {
	return db.engine.FindById(collection, id)
}

// DeleteById removes the document with the given id from the underlying collection, provided that such a document exists and satisfies the underlying query.
func (db *DB) DeleteById(collection string, id string) error {
	return db.engine.DeleteById(collection, id)
}

// UpdateById updates the document with the specified id using the supplied update map.
// If no document with the specified id exists, an ErrDocumentNotExist is returned.
func (db *DB) UpdateById(collection, docId string, updateMap map[string]interface{}) error {
	return db.engine.UpdateById(collection, docId, func(doc *d.Document) *d.Document {
		newDoc := doc.Copy()
		newDoc.SetAll(updateMap)
		return newDoc
	})
}

// ReplaceById replaces the document with the specified id with the one provided.
// If no document exists, an ErrDocumentNotExist is returned.
func (db *DB) ReplaceById(collection, docId string, doc *d.Document) error {
	if doc.ObjectId() != docId {
		return fmt.Errorf("the id of the document must match the one supplied")
	}
	return db.engine.UpdateById(collection, docId, func(_ *d.Document) *d.Document {
		return doc
	})
}

// Update updates all the document selected by q using the provided updateMap.
// Each update is specified by a mapping fieldName -> newValue.
func (db *DB) Update(q *query.Query, updateMap map[string]interface{}) error {
	q, err := normalizeCriteria(q)
	if err != nil {
		return err
	}

	return db.UpdateFunc(q, func(doc *d.Document) *d.Document {
		newDoc := doc.Copy()
		newDoc.SetAll(updateMap)
		return newDoc
	})
}

// Update updates all the document selected by q using the provided function.
func (db *DB) UpdateFunc(q *query.Query, updateFunc func(doc *d.Document) *d.Document) error {
	q, err := normalizeCriteria(q)
	if err != nil {
		return err
	}
	return db.engine.Update(q, updateFunc)
}

// Delete removes all the documents selected by q from the underlying collection.
func (db *DB) Delete(q *query.Query) error {
	q, err := normalizeCriteria(q)
	if err != nil {
		return err
	}
	return db.engine.Delete(q)
}

// ListCollections returns a slice of strings containing the name of each collection stored in the db.
func (db *DB) ListCollections() ([]string, error) {
	return db.engine.ListCollections()
}

// CreateIndex creates an index for the specified for the specified (index, collection) pair.
func (db *DB) CreateIndex(collection, field string) error {
	return db.engine.CreateIndex(collection, field)
}

// HasIndex returns true if an idex exists for the specified (index, collection) pair.
func (db *DB) HasIndex(collection, field string) (bool, error) {
	return db.engine.HasIndex(collection, field)
}

// DropIndex deletes the idex, is such index exists for the specified (index, collection) pair.
func (db *DB) DropIndex(collection, field string) error {
	return db.engine.DropIndex(collection, field)
}

// ListIndexes returns a list containing the names of all the indexes for the specified collection.
func (db *DB) ListIndexes(collection string) ([]index.IndexInfo, error) {
	return db.engine.ListIndexes(collection)
}

func normalizeCriteria(q *query.Query) (*query.Query, error) {
	if q.Criteria() != nil {
		v := &CriteriaNormalizeVisitor{}
		c := q.Criteria().Accept(v)

		if v.err != nil {
			return nil, v.err
		}

		q = q.Where(c.(query.Criteria))
	}
	return q, nil
}
