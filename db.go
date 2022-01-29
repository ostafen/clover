package clover

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"time"

	uuid "github.com/satori/go.uuid"
)

type DB struct {
	dir         string
	collections map[string]*Collection
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

func (db *DB) readCollection(name string) (*Collection, error) {
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

func (db *DB) Query(name string) *Collection {
	c, ok := db.collections[name]
	if !ok {
		return nil
	}
	return c
}

var ErrCollectionExist = errors.New("collection already exist")

func (db *DB) save(c *Collection) error {
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

func (db *DB) CreateCollection(name string) (*Collection, error) {
	if _, ok := db.collections[name]; ok {
		return nil, ErrCollectionExist
	}

	c := newCollection(db, name, nil)
	err := db.save(c)

	db.collections[name] = c
	return c, err
}

func (db *DB) DropCollection(name string) error {
	if _, ok := db.collections[name]; !ok {
		return ErrCollectionNotExist
	}

	delete(db.collections, name)
	return os.Remove(db.dir + "/" + name + ".json")
}

func (db *DB) HasCollection(name string) bool {
	_, ok := db.collections[name]
	return ok
}

func (db *DB) Insert(collectionName string, docs ...*Document) error {
	c, ok := db.collections[collectionName]
	if !ok {
		return ErrCollectionNotExist
	}

	for _, newDoc := range docs {
		uuid := uuid.NewV4().String()
		newDoc.Set(idFieldName, uuid)
		c.docs = append(c.docs, newDoc)
	}

	return db.save(c)
}

func (db *DB) InsertOne(collectionName string, doc *Document) (string, error) {
	err := db.Insert(collectionName, doc)
	return doc.Get(idFieldName).(string), err
}

func Open(dir string) (*DB, error) {
	if err := makeDirIfNotExists(dir); err != nil {
		return nil, err
	}

	db := &DB{
		dir:         dir,
		collections: make(map[string]*Collection),
	}
	return db, db.readCollections()
}
