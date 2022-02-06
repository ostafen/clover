package clover

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"time"

	uuid "github.com/satori/go.uuid"
)

// DB represents the entry point of each clover database.
type DB struct {
	dir         string
	collections map[string]*collection
}

type jsonFile struct {
	LastUpdate time.Time                `json:"last_update"`
	Rows       []map[string]interface{} `json:"rows"`
}

func rowsToDocuments(rows []map[string]interface{}) []*Document {
	docs := make([]*Document, 0, len(rows))
	for _, r := range rows {
		doc := NewDocument()
		doc.fields = r
		docs = append(docs, doc)
	}
	return docs
}

func (db *DB) readCollection(name string) (*collection, error) {
	data, err := ioutil.ReadFile(db.dir + "/" + name + ".json")
	if err != nil {
		return nil, err
	}

	jFile := &jsonFile{}
	if err := json.Unmarshal(data, jFile); err != nil {
		return nil, err
	}

	return newCollection(db, name, rowsToDocuments(jFile.Rows)), nil
}

var ErrCollectionNotExist = errors.New("no such collection")

// Query simply returns the collection with the supplied name. Use it to initialize a new query.
func (db *DB) Query(name string) *Query {
	c, ok := db.collections[name]
	if !ok {
		return nil
	}
	return &Query{collection: c, criteria: nil}
}

var ErrCollectionExist = errors.New("collection already exist")

func (db *DB) save(c *collection) error {
	docs := make([]map[string]interface{}, 0, c.Count())

	for _, d := range c.docs {
		docs = append(docs, d.fields)
	}

	jsonBytes, err := json.Marshal(&jsonFile{LastUpdate: time.Now(), Rows: docs})
	if err != nil {
		return err
	}
	return saveToFile(db.dir, c.name+".json", jsonBytes)
}

func (db *DB) readCollections() error {
	filenames, err := listDir(db.dir)
	if err != nil {
		return err
	}

	for _, filename := range filenames {
		collectionName := getBasename(filename)
		c, err := db.readCollection(collectionName)
		if err != nil {
			return err
		}
		db.collections[collectionName] = c
	}
	return nil
}

// CreateCollection creates a new empty collection with the given name.
func (db *DB) CreateCollection(name string) error {
	if _, ok := db.collections[name]; ok {
		return ErrCollectionExist
	}

	c := newCollection(db, name, nil)
	err := db.save(c)

	db.collections[name] = c
	return err
}

// Drop removes the collection with the given name, deleting any content on disk.
func (db *DB) DropCollection(name string) error {
	if _, ok := db.collections[name]; !ok {
		return ErrCollectionNotExist
	}

	delete(db.collections, name)
	return os.Remove(db.dir + "/" + name + ".json")
}

// HasCollections returns true if and only if the database contains a collection with the given name.
func (db *DB) HasCollection(name string) bool {
	_, ok := db.collections[name]
	return ok
}

func newObjectId() string {
	return uuid.NewV4().String()
}

// Insert adds the supplied documents to a collection.
func (db *DB) Insert(collectionName string, docs ...*Document) error {
	c, ok := db.collections[collectionName]
	if !ok {
		return ErrCollectionNotExist
	}

	insertDocs := make([]*Document, 0, len(docs))
	for _, doc := range docs {
		insertDoc := NewDocument()

		fields, err := normalizeMap(doc.fields)
		if err != nil {
			return err
		}
		insertDoc.fields = fields

		objectId := newObjectId()
		insertDoc.Set(objectIdField, objectId)
		doc.Set(objectIdField, objectId)

		insertDocs = append(insertDocs, insertDoc)
	}

	c.addDocuments(insertDocs...)

	return db.save(c)
}

// InsertOne inserts a single document to an existing collection. It returns the id of the inserted document.
func (db *DB) InsertOne(collectionName string, doc *Document) (string, error) {
	err := db.Insert(collectionName, doc)
	return doc.Get(objectIdField).(string), err
}

// Open opens a new clover database on the supplied path. If such a folder doesn't exist, it is automatically created.
func Open(dir string) (*DB, error) {
	if err := makeDirIfNotExists(dir); err != nil {
		return nil, err
	}

	db := &DB{
		dir:         dir,
		collections: make(map[string]*collection),
	}
	return db, db.readCollections()
}
