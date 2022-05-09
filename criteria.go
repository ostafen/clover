package clover

import (
	"regexp"
	"strings"

	"github.com/ostafen/clover/encoding"
)

const (
	objectIdField = "_id"
)

type predicate func(doc *Document) bool

var falseCriteria Criteria = Criteria{
	p: func(_ *Document) bool {
		return false
	},
}

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
			normalizedValue, err := encoding.Normalize(getFieldOrValue(doc, value))
			if err != nil {
				return false
			}
			if !doc.Has(f.name) {
				return false
			}
			return compareValues(doc.Get(f.name), normalizedValue) == 0
		},
	}
}

func (f *field) Gt(value interface{}) *Criteria {
	return &Criteria{
		p: func(doc *Document) bool {
			normValue, err := encoding.Normalize(getFieldOrValue(doc, value))
			if err != nil {
				return false
			}
			return compareValues(doc.Get(f.name), normValue) > 0
		},
	}
}

func (f *field) GtEq(value interface{}) *Criteria {
	return &Criteria{
		p: func(doc *Document) bool {
			normValue, err := encoding.Normalize(getFieldOrValue(doc, value))
			if err != nil {
				return false
			}
			return compareValues(doc.Get(f.name), normValue) >= 0
		},
	}
}

func (f *field) Lt(value interface{}) *Criteria {
	return &Criteria{
		p: func(doc *Document) bool {
			normValue, err := encoding.Normalize(getFieldOrValue(doc, value))
			if err != nil {
				return false
			}
			return compareValues(doc.Get(f.name), normValue) < 0
		},
	}
}

func (f *field) LtEq(value interface{}) *Criteria {
	return &Criteria{
		p: func(doc *Document) bool {
			normValue, err := encoding.Normalize(getFieldOrValue(doc, value))
			if err != nil {
				return false
			}
			return compareValues(doc.Get(f.name), normValue) <= 0
		},
	}
}

func (f *field) Neq(value interface{}) *Criteria {
	return f.Eq(value).Not()
}

func (f *field) In(values ...interface{}) *Criteria {
	return &Criteria{
		p: func(doc *Document) bool {
			normValues := make([]interface{}, 0, len(values))
			for _, v := range values {
				normValue, err := encoding.Normalize(getFieldOrValue(doc, v))
				if err != nil {
					return false
				}
				normValues = append(normValues, normValue)
			}
			docValue := doc.Get(f.name)
			for _, value := range normValues {
				if compareValues(value, docValue) == 0 {
					return true
				}
			}
			return false
		},
	}
}

func (f *field) Contains(elems ...interface{}) *Criteria {
	return &Criteria{
		p: func(doc *Document) bool {
			fieldValue := doc.Get(f.name)
			slice, _ := fieldValue.([]interface{})

			if fieldValue == nil || slice == nil {
				return false
			}

			for _, elem := range elems {
				found := false
				normElem, err := encoding.Normalize(getFieldOrValue(doc, elem))

				if err == nil {
					for _, val := range slice {
						if compareValues(normElem, val) == 0 {
							found = true
							break
						}
					}
				}

				if !found {
					return false
				}

			}
			return true
		},
	}
}

func (f *field) Like(pattern string) *Criteria {
	expr, err := regexp.Compile(pattern)
	if err != nil {
		return &falseCriteria
	}

	return &Criteria{
		p: func(doc *Document) bool {
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

// getFieldOrValue returns dereferenced value if value denotes another document field,
// otherwise returns the value itself directly
func getFieldOrValue(doc *Document, value interface{}) interface{} {
	if cmpField, ok := value.(*field); ok {
		value = doc.Get(cmpField.name)
	} else if fStr, ok := value.(string); ok && strings.HasPrefix(fStr, "$") {
		fieldName := strings.TrimLeft(fStr, "$")
		value = doc.Get(fieldName)
	}
	return value
}
