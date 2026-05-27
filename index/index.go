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
	Directions() []bool
}

type indexBase struct {
	collection string
	fields     []string
	// directions holds per‑field sort direction: true = ascending, false = descending.
	// If nil, all fields are treated as ascending (default behavior).
	directions []bool
}

func (idx *indexBase) Collection() string {
	return idx.collection
}

func (idx *indexBase) Fields() []string {
	return idx.fields
}

func (idx *indexBase) Directions() []bool {
	if idx.directions != nil {
		return idx.directions
	}
	dirs := make([]bool, len(idx.fields))
	for i := range dirs {
		dirs[i] = true
	}
	return dirs
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

// CreateIndexWithDirections creates a compound index where each field can be
// individually ascending (true) or descending (false). The length of `asc`
// must match the number of `fields`. If the lengths differ, the function panics
// because this is a programmer error and should be caught during development.
func CreateIndexWithDirections(collection string, fields []string, asc []bool, tx store.Tx) Index {
	if len(fields) != len(asc) {
		panic("CreateIndexWithDirections: fields and asc slices must have the same length")
	}
	indexBase := indexBase{collection: collection, fields: fields, directions: asc}
	// Use Compound type for mixed direction indexes.
	return &rangeIndex{
		indexBase: indexBase,
		tx:        tx,
	}
}
