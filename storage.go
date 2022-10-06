package clover

import (
	"errors"

	d "github.com/ostafen/clover/v2/document"
	"github.com/ostafen/clover/v2/index"
	"github.com/ostafen/clover/v2/query"
)

var ErrDocumentNotExist = errors.New("no such document")
var ErrDuplicateKey = errors.New("duplicate key")

type docConsumer func(doc *d.Document) error

// StorageEngine represents the persistance layer and abstracts how collections are stored.
type StorageEngine interface {
	CreateCollection(name string) error
	ListCollections() ([]string, error)
	DropCollection(name string) error
	HasCollection(name string) (bool, error)
	Count(q *query.Query) (int, error)
	FindAll(q *query.Query) ([]*d.Document, error)
	FindById(collectionName string, id string) (*d.Document, error)
	UpdateById(collectionName string, docId string, updater func(doc *d.Document) *d.Document) error
	DeleteById(collectionName string, id string) error
	IterateDocs(q *query.Query, consumer docConsumer) error
	Insert(collection string, docs ...*d.Document) error
	Update(q *query.Query, updater func(doc *d.Document) *d.Document) error
	Delete(q *query.Query) error
	CreateIndex(collection, field string) error
	DropIndex(collection, field string) error
	HasIndex(collection, field string) (bool, error)
	ListIndexes(collection string) ([]index.IndexInfo, error)
	Close() error
}
