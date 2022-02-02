package clover

import (
	"encoding/json"
	"reflect"
	"strings"
)

const (
	idFieldName = "_id"
)

type predicate func(doc *Document) bool

type Criteria struct {
	p predicate
}

type Collection struct {
	db       *DB
	name     string
	docs     map[string]*Document
	criteria *Criteria
}

func (c *Collection) Count() int {
	return len(c.docs)
}

func (c *Collection) FindAll() []*Document {
	docs := make([]*Document, 0, len(c.docs))
	for _, doc := range c.docs {
		docs = append(docs, doc)
	}
	return docs
}

func newCollection(db *DB, name string, docs []*Document) *Collection {
	c := &Collection{
		db:       db,
		name:     name,
		docs:     make(map[string]*Document),
		criteria: nil,
	}
	c.addDocuments(docs...)
	return c
}

func (c *Collection) addDocuments(docs ...*Document) {
	for _, doc := range docs {
		c.docs[doc.Get(idFieldName).(string)] = doc
	}
}

type row struct {
	name string
}

func Row(name string) *row {
	return &row{name: name}
}

func (r *row) Exists() *Criteria {
	return &Criteria{
		p: func(doc *Document) bool {
			return doc.Has(r.name)
		},
	}
}

func (r *row) Eq(value interface{}) *Criteria {
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

func (r *row) Gt(value interface{}) *Criteria {
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

func (r *row) GtEq(value interface{}) *Criteria {
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

func (r *row) Lt(value interface{}) *Criteria {
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

func (r *row) LtEq(value interface{}) *Criteria {
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

func (r *row) Neq(value interface{}) *Criteria {
	c := r.Eq(value)
	return c.Not()
}

func (r *row) In(values ...interface{}) *Criteria {
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

func (c *Collection) Where(q *Criteria) *Collection {
	filtered := make([]*Document, 0)
	for _, doc := range c.docs {
		if q.p(doc) {
			filtered = append(filtered, doc)
		}
	}
	newColl := newCollection(c.db, c.name, filtered)
	newColl.criteria = q
	return newColl
}

func (c *Collection) Matches(predicate func(doc *Document) bool) *Collection {
	return c.Where(&Criteria{
		p: predicate,
	})
}

func (c *Collection) FindById(id string) *Document {
	return c.docs[id]
}

func (c *Collection) Update(updateMap map[string]interface{}) error {
	updatedDocs := make([]*Document, 0, len(c.docs))
	for _, doc := range c.docs {
		updateDoc := doc.Copy()
		for updateField, updateValue := range updateMap {
			updateDoc.Set(updateField, updateValue)
		}
		updatedDocs = append(updatedDocs, updateDoc)
	}

	// copy collection
	baseColl := c.db.collections[c.name]
	for _, doc := range updatedDocs {
		baseColl.docs[doc.Get(idFieldName).(string)] = doc
	}
	return baseColl.db.save(baseColl)
}

func (c *Collection) DeleteById(id string) error {
	return nil
}

func (c *Collection) Delete() error {
	newColl := c.db.Query(c.name)

	if c.criteria != nil {
		newColl = newColl.Where(c.criteria.Not())
	}

	if err := c.db.save(newColl); err != nil {
		return err
	}
	c.db.collections[c.name] = newColl
	return nil
}

type Document struct {
	fields map[string]interface{}
}

func NewDocument() *Document {
	return &Document{
		fields: make(map[string]interface{}),
	}
}

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

func (doc *Document) Has(name string) bool {
	fieldMap, _, _ := lookupField(name, doc.fields, false)
	return fieldMap != nil
}

func (doc *Document) Get(name string) interface{} {
	_, v, _ := lookupField(name, doc.fields, false)
	return v
}

func (doc *Document) Set(name string, value interface{}) {
	m, _, fieldName := lookupField(name, doc.fields, true)
	m[fieldName] = value
}

func (doc *Document) Unmarshal(value interface{}) error {
	bytes, err := json.Marshal(doc.fields)
	if err != nil {
		return err
	}
	return json.Unmarshal(bytes, value)
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
	case int64:
		return float64(i)
	case int32:
		return float64(i)
	case int:
		return float64(i)
	case uint64:
		return float64(i)
	case uint32:
		return float64(i)
	case uint:
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
