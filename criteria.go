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

type Predicate interface {
	Satisfy(doc *Document) bool
}

const (
	ExistsOp = iota
	EqOp
	GtOp
	GtEqOp
	LtOp
	LtEqOp
	LikeOp
	InOp
	ContainsOp
	FunctionOp
)

const (
	LogicalAnd = iota
	LogicalOr
)

// Criteria represents a predicate for selecting documents.
// It follows a fluent API style so that you can easily chain together multiple criteria.
type Criteria interface {
	Predicate
	Not() Criteria
	And(c Criteria) Criteria
	Or(c Criteria) Criteria
}

type binaryPredicate struct {
	opType int
	c1, c2 Predicate
}

func (p *binaryPredicate) Satisfy(doc *Document) bool {
	if p.opType == LogicalAnd {
		return p.c1.Satisfy(doc) && p.c2.Satisfy(doc)
	}
	return p.c1.Satisfy(doc) || p.c2.Satisfy(doc)
}

type predicateDecorator struct {
	negate bool
	p      Predicate
}

func (c *predicateDecorator) Satisfy(doc *Document) bool {
	res := c.p.Satisfy(doc)
	if c.negate {
		return !res
	}
	return res
}

func (c *predicateDecorator) Not() Criteria {
	return &predicateDecorator{
		negate: !c.negate,
		p:      c.p,
	}
}

func (c *predicateDecorator) And(p Criteria) Criteria {
	return &predicateDecorator{
		p: &binaryPredicate{
			opType: LogicalAnd,
			c1:     c.p,
			c2:     p,
		},
	}
}

func (c *predicateDecorator) Or(p Criteria) Criteria {
	return &predicateDecorator{
		p: &binaryPredicate{
			opType: LogicalOr,
			c1:     c.p,
			c2:     p,
		},
	}
}

type simplePredicate struct {
	opType int
	field  string
	value  interface{}
}

func newCriterion(opType int, field string, value interface{}) Criteria {
	return &predicateDecorator{
		negate: false,
		p: &simplePredicate{
			opType: opType,
			field:  field,
			value:  value,
		},
	}
}

func (c *simplePredicate) Satisfy(doc *Document) bool {
	switch c.opType {
	case ExistsOp:
		return c.exist(doc)
	case EqOp:
		return c.eq(doc)
	case LikeOp:
		return c.like(doc)
	case InOp:
		return c.in(doc)
	case GtOp, GtEqOp, LtOp, LtEqOp:
		return c.compare(doc)
	case ContainsOp:
		return c.contains(doc)
	case FunctionOp:
		return c.value.(func(*Document) bool)(doc)
	}
	return false
}

func (c *simplePredicate) exist(doc *Document) bool {
	return doc.Has(c.field)
}

func (c *simplePredicate) notExists(doc *Document) bool {
	return !c.exist(doc)
}

func (c *simplePredicate) compare(doc *Document) bool {
	normValue, err := encoding.Normalize(getFieldOrValue(doc, c.value))
	if err != nil {
		return false
	}

	res := compareValues(doc.Get(c.field), normValue)

	switch c.opType {
	case GtOp:
		return res > 0
	case GtEqOp:
		return res >= 0
	case LtOp:
		return res < 0
	case LtEqOp:
		return res <= 0
	}
	panic("unreachable code")
}

func (c *simplePredicate) eq(doc *Document) bool {
	normValue, err := encoding.Normalize(getFieldOrValue(doc, c.value))
	if err != nil {
		return false
	}

	if !doc.Has(c.field) {
		return false
	}

	return compareValues(doc.Get(c.field), normValue) == 0
}

func (c *simplePredicate) in(doc *Document) bool {
	values := c.value.([]interface{})

	docValue := doc.Get(c.field)
	for _, value := range values {
		normValue, err := encoding.Normalize(getFieldOrValue(doc, value))
		if err == nil && compareValues(normValue, docValue) == 0 {
			return true
		}
	}
	return false
}

func (c *simplePredicate) contains(doc *Document) bool {
	elems := c.value.([]interface{})

	fieldValue := doc.Get(c.field)
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
}

func (c *simplePredicate) like(doc *Document) bool {
	pattern := c.value.(string)

	s, isString := doc.Get(c.field).(string)
	if !isString {
		return false
	}
	matched, err := regexp.MatchString(pattern, s)
	return matched && err == nil
}

type field struct {
	name string
}

// Field represents a document field. It is used to create a new criteria.
func Field(name string) *field {
	return &field{name: name}
}

func (f *field) Exists() Criteria {
	return newCriterion(ExistsOp, f.name, nil)
}

func (f *field) NotExists() Criteria {
	return newCriterion(ExistsOp, f.name, nil).Not()
}

func (f *field) IsNil() Criteria {
	return f.Eq(nil)
}

func (f *field) IsTrue() Criteria {
	return f.Eq(true)
}

func (f *field) IsFalse() Criteria {
	return f.Eq(false)
}

func (f *field) IsNilOrNotExists() Criteria {
	return f.IsNil().Or(f.NotExists())
}

func (f *field) Eq(value interface{}) Criteria {
	return newCriterion(EqOp, f.name, value)
}

func (f *field) Gt(value interface{}) Criteria {
	return newCriterion(GtOp, f.name, value)
}

func (f *field) GtEq(value interface{}) Criteria {
	return newCriterion(GtEqOp, f.name, value)
}

func (f *field) Lt(value interface{}) Criteria {
	return newCriterion(LtOp, f.name, value)
}

func (f *field) LtEq(value interface{}) Criteria {
	return newCriterion(LtEqOp, f.name, value)
}

func (f *field) Neq(value interface{}) Criteria {
	return f.Eq(value).Not()
}

func (f *field) In(values ...interface{}) Criteria {
	return newCriterion(InOp, f.name, values)
}

func (f *field) Like(pattern string) Criteria {
	return newCriterion(LikeOp, f.name, pattern)
}

func (f *field) Contains(elems ...interface{}) Criteria {
	return newCriterion(ContainsOp, f.name, elems)
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
