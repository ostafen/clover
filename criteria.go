package clover

import (
	"reflect"
	"regexp"
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

type field struct {
	name string
}

// Field represents a document field. It is used to create a new criteria.
func Field(name string) *field {
	return &field{name: name}
}

func (f *field) Exists() *Criteria {
	return &Criteria{
		p: func(doc *Document) bool {
			return doc.Has(f.name)
		},
	}
}

func (f *field) NotExists() *Criteria {
	return f.Exists().Not()
}

func (f *field) IsNil() *Criteria {
	return f.Eq(nil)
}

func (f *field) IsTrue() *Criteria {
	return f.Eq(true)
}

func (f *field) IsFalse() *Criteria {
	return f.Eq(false)
}

func (f *field) IsNilOrNotExists() *Criteria {
	return f.IsNil().Or(f.NotExists())
}

func (f *field) Eq(value interface{}) *Criteria {
	return &Criteria{
		p: func(doc *Document) bool {
			normValue, err := normalize(value)
			if err != nil {
				return false
			}

			if !doc.Has(f.name) {
				return false
			}
			return reflect.DeepEqual(doc.Get(f.name), normValue)
		},
	}
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

func (f *field) Like(pattern string) *Criteria {
	expr, err := regexp.Compile(pattern)

	return &Criteria{
		p: func(doc *Document) bool {
			if err != nil {
				return false
			}

			s, isString := doc.Get(f.name).(string)
			if !isString {
				return false
			}
			matched := expr.MatchString(s)
			return matched
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
