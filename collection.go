package clover

import (
	"encoding/json"
	"reflect"
	"strings"
)

type predicate func(doc *Document) bool

type Criteria struct {
	p predicate
}

type Collection struct {
	name string
	docs []*Document
}

func (c *Collection) Count() int {
	return len(c.docs)
}

func (c *Collection) FindAll() []*Document {
	return c.docs
}

func newCollection(docs []*Document) *Collection {
	return &Collection{
		docs: docs,
	}
}

type Row struct {
	name string
}

func row(name string) *Row { // must return something else
	return &Row{name: name}
}

func (r *Row) Eq(value interface{}) *Criteria {
	return &Criteria{
		p: func(doc *Document) bool {
			normValue, err := normalize(value)
			if err != nil {
				return false
			}
			return reflect.DeepEqual(doc.get(r.name), normValue)
		},
	}
}

func compareValues(v1 interface{}, v2 interface{}) int {
	v1Float, isFloat := v1.(float64)
	if isFloat {
		v2Float, isFloat := v2.(float64)
		if isFloat {
			return int(v1Float - v2Float)
		}
	}
	return 0
}

func (r *Row) Gt(value interface{}) *Criteria {
	return &Criteria{
		p: func(doc *Document) bool {
			normValue, err := normalize(value)
			if err != nil {
				return false
			}
			return compareValues(doc.get(r.name), normValue) > 0
		},
	}
}

func (r *Row) GtEq(value interface{}) *Criteria {
	return &Criteria{
		p: func(doc *Document) bool {
			normValue, err := normalize(value)
			if err != nil {
				return false
			}
			return compareValues(doc.get(r.name), normValue) >= 0
		},
	}
}

func (r *Row) Lt(value interface{}) *Criteria {
	return &Criteria{
		p: func(doc *Document) bool {
			normValue, err := normalize(value)
			if err != nil {
				return false
			}
			return compareValues(doc.get(r.name), normValue) < 0
		},
	}
}

func (r *Row) LtEq(value interface{}) *Criteria {
	return &Criteria{
		p: func(doc *Document) bool {
			normValue, err := normalize(value)
			if err != nil {
				return false
			}
			return compareValues(doc.get(r.name), normValue) <= 0
		},
	}
}

func (r *Row) Neq(value interface{}) *Criteria {
	c := r.Eq(value)
	return c.Not()
}

func (r *Row) In(values ...interface{}) *Criteria {
	return &Criteria{
		p: func(doc *Document) bool {
			docValue := doc.get(r.name)
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
	return newCollection(filtered)
}

func (c *Collection) FindById(id string) *Document {
	for _, doc := range c.docs {
		docId := doc.get("_id")
		if docId != nil && docId == id {
			return doc
		}
	}
	return nil
}

type Document struct {
	fields map[string]interface{}
}

func newDocument() *Document {
	return &Document{
		fields: make(map[string]interface{}),
	}
}

func lookupField(name string, fieldMap map[string]interface{}) interface{} {
	fields := strings.Split(name, ".")

	var f interface{}
	currMap := fieldMap
	for i, field := range fields {
		f = currMap[field]

		if f == nil {
			return nil
		}

		if m, ok := f.(map[string]interface{}); ok {
			currMap = m
		} else {
			if i < len(fields)-1 {
				return nil
			}
		}
	}
	return f
}

func (doc *Document) get(name string) interface{} {
	return lookupField(name, doc.fields)
}

func (doc *Document) set(name string, value interface{}) {
	normValue, _ := normalize(value)
	doc.fields[name] = normValue
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
