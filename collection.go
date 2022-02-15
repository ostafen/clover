package clover

import (
	"encoding/json"
	"reflect"
	"strings"
)

const (
	objectIdField = "_id"
)

type predicate func(doc *Document) bool

// Criteria represents a predicate for selecting documents.
// It follows a fluent API style so that you can easily chain together multiple criteria.
type Criteria struct {
	p predicate
}

// collection represents a set of documents. It contains methods to add, select or delete documents.
type collection struct {
	db       *DB
	name     string
	docs     map[string]*Document
	criteria *Criteria
}

// Count returns the number of documents stored in the given collection.
func (c *collection) Count() int {
	return len(c.docs)
}

func newCollection(db *DB, name string, docs []*Document) *collection {
	c := &collection{
		db:       db,
		name:     name,
		docs:     make(map[string]*Document),
		criteria: nil,
	}
	c.addDocuments(docs...)
	return c
}

func (c *collection) addDocuments(docs ...*Document) {
	for _, doc := range docs {
		c.docs[doc.Get(objectIdField).(string)] = doc
	}
}

type field struct {
	name string
}

// Field represents a document field. It is used to create a new criteria.
func Field(name string) *field {
	return &field{name: name}
}

func (r *field) Exists() *Criteria {
	return &Criteria{
		p: func(doc *Document) bool {
			return doc.Has(r.name)
		},
	}
}

func (r *field) Eq(value interface{}) *Criteria {
	return &Criteria{
		p: func(doc *Document) bool {
			normValue, err := normalize(value)
			if err != nil {
				return false
			}
			return reflect.DeepEqual(doc.Get(r.name), normValue)
		},
	}
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func compareValues(v1 interface{}, v2 interface{}) (int, bool) {
	v1Float, isFloat := v1.(float64)
	if isFloat {
		v2Float, isFloat := v2.(float64)
		if isFloat {
			return int(v1Float - v2Float), true
		}
	}

	v1Str, isStr := v1.(string)
	if isStr {
		v2Str, isStr := v2.(string)
		if isStr {
			return strings.Compare(v1Str, v2Str), true
		}
	}

	v1Bool, isBool := v1.(bool)
	if isBool {
		v2Bool, isBool := v2.(bool)
		if isBool {
			return boolToInt(v1Bool) - boolToInt(v2Bool), true
		}
	}

	return 0, false
}

func (f *field) Gt(value interface{}) *Criteria {
	return &Criteria{
		p: func(doc *Document) bool {
			normValue, err := normalize(value)
			if err != nil {
				return false
			}
			v, ok := compareValues(doc.Get(f.name), normValue)
			if !ok {
				return false
			}
			return v > 0
		},
	}
}

func (f *field) GtEq(value interface{}) *Criteria {
	return &Criteria{
		p: func(doc *Document) bool {
			normValue, err := normalize(value)
			if err != nil {
				return false
			}
			v, ok := compareValues(doc.Get(f.name), normValue)
			if !ok {
				return false
			}
			return v >= 0
		},
	}
}

func (f *field) Lt(value interface{}) *Criteria {
	return &Criteria{
		p: func(doc *Document) bool {
			normValue, err := normalize(value)
			if err != nil {
				return false
			}
			v, ok := compareValues(doc.Get(f.name), normValue)
			if !ok {
				return false
			}
			return v < 0
		},
	}
}

func (f *field) LtEq(value interface{}) *Criteria {
	return &Criteria{
		p: func(doc *Document) bool {
			normValue, err := normalize(value)
			if err != nil {
				return false
			}
			v, ok := compareValues(doc.Get(f.name), normValue)
			if !ok {
				return false
			}
			return v <= 0
		},
	}
}

func (f *field) Neq(value interface{}) *Criteria {
	c := f.Eq(value)
	return c.Not()
}

func (f *field) In(values ...interface{}) *Criteria {
	return &Criteria{
		p: func(doc *Document) bool {
			docValue := doc.Get(f.name)
			for _, value := range values {
				normValue, err := normalize(value)
				if err == nil {
					if reflect.DeepEqual(normValue, docValue) {
						return true
					}
				}
			}
			return false
		},
	}
}

func negatePredicate(p predicate) predicate {
	return func(doc *Document) bool {
		return !p(doc)
	}
}

func andPredicates(p1 predicate, p2 predicate) predicate {
	return func(doc *Document) bool {
		return p1(doc) && p2(doc)
	}
}

func orPredicates(p1 predicate, p2 predicate) predicate {
	return func(doc *Document) bool {
		return p1(doc) || p2(doc)
	}
}

// And returns a new Criteria obtained by combining the predicates of the provided criteria with the AND logical operator.
func (c *Criteria) And(other *Criteria) *Criteria {
	return &Criteria{
		p: andPredicates(c.p, other.p),
	}
}

// Or returns a new Criteria obtained by combining the predicates of the provided criteria with the OR logical operator.
func (c *Criteria) Or(other *Criteria) *Criteria {
	return &Criteria{
		p: orPredicates(c.p, other.p),
	}
}

// Not returns a new Criteria which negate the predicate of the original criterion.
func (c *Criteria) Not() *Criteria {
	return &Criteria{
		p: negatePredicate(c.p),
	}
}

func normalize(value interface{}) (interface{}, error) {
	var normalized interface{}
	bytes, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(bytes, &normalized)
	return normalized, err
}
