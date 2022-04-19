package clover

import (
	"errors"

	uuid "github.com/satori/go.uuid"
)

// Collection creation errors
var (
	ErrCollectionExist    = errors.New("collection already exist")
	ErrCollectionNotExist = errors.New("no such collection")
)

// DB represents the entry point of each clover database.
type DB struct {
	dir    string
	engine StorageEngine
}

// Query simply returns the collection with the supplied name. Use it to initialize a new query.
func (db *DB) Query(name string) *Query {
	return &Query{
		collection: name,
		criteria:   nil,
		engine:     db.engine,
		limit:      -1,
		skip:       0,
		sortOpts:   nil,
	}
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

func newObjectId() string {
	return uuid.NewV4().String()
}

// Insert adds the supplied documents to a collection.
func (db *DB) Insert(collectionName string, docs ...*Document) error {
	insertDocs := make([]*Document, 0, len(docs))
	for _, doc := range docs {
		insertDoc := NewDocument()
		fields, err := normalize(doc.fields)
		if err != nil {
			return err
		}
		insertDoc.fields = fields.(map[string]interface{})

		objectId := newObjectId()
		insertDoc.Set(objectIdField, objectId)
		doc.Set(objectIdField, objectId)

		insertDocs = append(insertDocs, insertDoc)
	}
	return db.engine.Insert(collectionName, insertDocs...)
}

// InsertOne inserts a single document to an existing collection. It returns the id of the inserted document.
func (db *DB) InsertOne(collectionName string, doc *Document) (string, error) {
	err := db.Insert(collectionName, doc)
	return doc.Get(objectIdField).(string), err
}

// Open opens a new clover database on the supplied path. If such a folder doesn't exist, it is automatically created.
func Open(dir string, opts ...Option) (*DB, error) {
	config, err := defaultConfig().applyOptions(opts)
	if err != nil {
		return nil, err
	}

	if !config.InMemory {
		if err := makeDirIfNotExists(dir); err != nil {
			return nil, err
		}
	}

	db := &DB{
		dir:    dir,
		engine: config.Storage,
	}
	return db, db.engine.Open(dir)
}

// Close releases all the resources and closes the database. After the call, the instance will no more be usable.
func (db *DB) Close() error {
	return db.engine.Close()
}

// Returns a slice of strings with the collection's names
func (db *DB) ListCollections() ([]string, error) {
	return db.engine.ListCollections()
}
