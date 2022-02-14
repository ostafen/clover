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

// Query represents a generic query which is submitted to a specific collection.
type Query struct {
	collection *collection
	criteria   *Criteria
}

func (q *Query) satisfy(doc *Document) bool {
	if q.criteria == nil {
		return true
	}
	return q.criteria.p(doc)
}

// Count returns the number of documents which satisfy the query (i.e. len(q.FindAll()) == q.Count()).
func (q *Query) Count() int {
	n := 0
	for _, doc := range q.collection.docs {
		if q.satisfy(doc) {
			n++
		}
	}
	return n
}

// MatchPredicate selects all the documents which satisfy the supplied predicate function.
func (q *Query) MatchPredicate(p func(doc *Document) bool) *Query {
	return q.Where(&Criteria{p})
}

// Where returns a new Query which select all the documents fullfilling both the base query and the provided Criteria.
func (q *Query) Where(c *Criteria) *Query {
	newCriteria := q.criteria
	if newCriteria == nil {
		newCriteria = c
	} else {
		newCriteria = newCriteria.And(c)
	}

	return &Query{
		collection: q.collection,
		criteria:   newCriteria,
	}
}

// FindById returns the document with the given id, if such a document exists and satifies the underlying query, or null.
func (q *Query) FindById(id string) *Document {
	doc, ok := q.collection.docs[id]
	if ok && q.satisfy(doc) {
		return doc
	}
	return nil
}

// FindAll selects all the documents satisfying q.
func (q *Query) FindAll() []*Document {
	docs := make([]*Document, 0)
	for _, doc := range q.collection.docs {
		if q.satisfy(doc) {
			docs = append(docs, doc)
		}
	}
	return docs
}

// Update updates all the document selected by q using the provided updateMap.
// Each update is specified by a mapping fieldName -> newValue.
func (q *Query) Update(updateMap map[string]interface{}) error {
	for _, doc := range q.collection.docs {
		if q.criteria.p(doc) {
			updateDoc := doc.Copy()
			for updateField, updateValue := range updateMap {
				updateDoc.Set(updateField, updateValue)
			}
			q.collection.docs[updateDoc.Get(objectIdField).(string)] = updateDoc
		}
	}
	return q.collection.db.save(q.collection)
}

// DeleteById removes the document with the given id from the underlying collection, provided that such a document exists and satifies the underlying query.
func (q *Query) DeleteById(id string) error {
	doc, ok := q.collection.docs[id]
	if ok && q.satisfy(doc) {
		delete(q.collection.docs, doc.Get(objectIdField).(string))
		return q.collection.db.save(q.collection)
	}
	return nil
}

// Delete removes all the documents selected by q from the underlying collection.
func (q *Query) Delete() error {
	for _, doc := range q.collection.docs {
		if q.satisfy(doc) {
			delete(q.collection.docs, doc.Get(objectIdField).(string))
		}
	}
	return q.collection.db.save(q.collection)
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

func (r *field) Gt(value interface{}) *Criteria {
	return &Criteria{
		p: func(doc *Document) bool {
			normValue, err := normalize(value)
			if err != nil {
				return false
			}
			v, ok := compareValues(doc.Get(r.name), normValue)
			if !ok {
				return false
			}
			return v > 0
		},
	}
}

func (r *field) GtEq(value interface{}) *Criteria {
	return &Criteria{
		p: func(doc *Document) bool {
			normValue, err := normalize(value)
			if err != nil {
				return false
			}
			v, ok := compareValues(doc.Get(r.name), normValue)
			if !ok {
				return false
			}
			return v >= 0
		},
	}
}

func (r *field) Lt(value interface{}) *Criteria {
	return &Criteria{
		p: func(doc *Document) bool {
			normValue, err := normalize(value)
			if err != nil {
				return false
			}
			v, ok := compareValues(doc.Get(r.name), normValue)
			if !ok {
				return false
			}
			return v < 0
		},
	}
}

func (r *field) LtEq(value interface{}) *Criteria {
	return &Criteria{
		p: func(doc *Document) bool {
			normValue, err := normalize(value)
			if err != nil {
				return false
			}
			v, ok := compareValues(doc.Get(r.name), normValue)
			if !ok {
				return false
			}
			return v <= 0
		},
	}
}

func (r *field) Neq(value interface{}) *Criteria {
	c := r.Eq(value)
	return c.Not()
}

func (r *field) In(values ...interface{}) *Criteria {
	return &Criteria{
		p: func(doc *Document) bool {
			docValue := doc.Get(r.name)
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

func (q *Criteria) And(other *Criteria) *Criteria {
	return &Criteria{
		p: andPredicates(q.p, other.p),
	}
}

func (q *Criteria) Or(other *Criteria) *Criteria {
	return &Criteria{
		p: orPredicates(q.p, other.p),
	}
}

func (q *Criteria) Not() *Criteria {
	return &Criteria{
		p: negatePredicate(q.p),
	}
}

type Document struct {
	fields map[string]interface{}
}

// ObjectId returns the id of the document, provided that the document belongs to some collection. Otherwise, it returns the empty string.
func (doc *Document) ObjectId() string {
	id := doc.Get(objectIdField)
	if id == nil {
		return ""
	}
	return id.(string)
}

// NewDocument creates a new empty document.
func NewDocument() *Document {
	return &Document{
		fields: make(map[string]interface{}),
	}
}

// Copy returns a shallow copy of the underlying document.
func (doc *Document) Copy() *Document {
	return &Document{
		fields: copyMap(doc.fields),
	}
}

func lookupField(name string, fieldMap map[string]interface{}, force bool) (map[string]interface{}, interface{}, string) {
	fields := strings.Split(name, ".")

	var exists bool
	var f interface{}
	currMap := fieldMap
	for i, field := range fields {
		f, exists = currMap[field]

		m, isMap := f.(map[string]interface{})

		if force {
			if (!exists || !isMap) && i < len(fields)-1 {
				m = make(map[string]interface{})
				currMap[field] = m
				f = m
			}
		} else if !exists {
			return nil, nil, ""
		}

		if i < len(fields)-1 {
			currMap = m
		}
	}
	return currMap, f, fields[len(fields)-1]
}

// Has tells returns true if the document contains a field with the supplied name.
func (doc *Document) Has(name string) bool {
	fieldMap, _, _ := lookupField(name, doc.fields, false)
	return fieldMap != nil
}

// Get retrieves the value of a field. Nested fields can be accessed using dot.
func (doc *Document) Get(name string) interface{} {
	_, v, _ := lookupField(name, doc.fields, false)
	return v
}

// Set maps a field to a value. Nested fields can be accessed using dot.
func (doc *Document) Set(name string, value interface{}) {
	m, _, fieldName := lookupField(name, doc.fields, true)
	m[fieldName] = value
}

// Unmarshal stores the document in the value pointed by v.
func (doc *Document) Unmarshal(v interface{}) error {
	bytes, err := json.Marshal(doc.fields)
	if err != nil {
		return err
	}
	return json.Unmarshal(bytes, v)
}

func normalizeMap(data interface{}) (map[string]interface{}, error) {
	m := make(map[string]interface{})
	bytes, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(bytes, &m)
	return m, err
}

func normalizeSlice(value interface{}) ([]interface{}, error) {
	s := make([]interface{}, 0)
	bytes, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(bytes, &s)
	return s, err
}

func normalizeSimpleValue(value interface{}) interface{} {
	switch i := value.(type) {
	case float64:
		return i
	case float32:
		return float64(i)
	case int:
		return float64(i)
	case int8:
		return float64(i)
	case int16:
		return float64(i)
	case int32:
		return float64(i)
	case int64:
		return float64(i)
	case uint:
		return float64(i)
	case uint8:
		return float64(i)
	case uint16:
		return float64(i)
	case uint32:
		return float64(i)
	case uint64:
		return float64(i)
	}
	return value
}

func normalize(value interface{}) (interface{}, error) {
	kind := reflect.TypeOf(value).Kind()
	switch kind {
	case reflect.Struct:
		return normalizeMap(value)
	case reflect.Slice:
		return normalizeSlice(value)
	}
	return normalizeSimpleValue(value), nil
}
