package index

import (
	"time"

	"github.com/ostafen/clover/v2/store"
)

type Type int

const (
	SingleField Type = iota
	Compound
)

type Info struct {
	Fields []string
	Type   Type
}

type Index interface {
	Add(docId string, values []interface{}, ttl time.Duration) error
	Remove(docId string, values []interface{}) error
	Iterate(reverse bool, onValue func(docId string) error) error
	Drop() error
	Type() Type
	Collection() string
	Fields() []string
}

type indexBase struct {
	collection string
	fields     []string
}

func (idx *indexBase) Collection() string {
	return idx.collection
}

func (idx *indexBase) Fields() []string {
	return idx.fields
}

type Query interface {
	Run(onValue func(docId string) error) error
}

func CreateIndex(collection string, fields []string, idxType Type, tx store.Tx) Index {
	indexBase := indexBase{collection: collection, fields: fields}
	switch idxType {
	case SingleField, Compound:
		return &rangeIndex{
			indexBase: indexBase,
			tx:        tx,
		}
	}
	return nil
}
