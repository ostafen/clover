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
	p1, p2 Predicate
}

func (p *binaryPredicate) Satisfy(doc *Document) bool {
	if p.opType == LogicalAnd {
		return p.p1.Satisfy(doc) && p.p2.Satisfy(doc)
	}
	return p.p1.Satisfy(doc) || p.p2.Satisfy(doc)
}

type predicateDecorator struct {
	negate bool
	p      Predicate
}

func (p *predicateDecorator) Satisfy(doc *Document) bool {
	res := p.p.Satisfy(doc)
	if p.negate {
		return !res
	}
	return res
}

func (p *predicateDecorator) Not() Criteria {
	return &predicateDecorator{
		negate: !p.negate,
		p:      p.p,
	}
}

func (p *predicateDecorator) And(c Criteria) Criteria {
	return &predicateDecorator{
		p: &binaryPredicate{
			opType: LogicalAnd,
			p1:     p.p,
			p2:     c,
		},
	}
}

func (p *predicateDecorator) Or(c Criteria) Criteria {
	return &predicateDecorator{
		p: &binaryPredicate{
			opType: LogicalOr,
			p1:     p.p,
			p2:     c,
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

func (p *simplePredicate) Satisfy(doc *Document) bool {
	switch p.opType {
	case ExistsOp:
		return p.exist(doc)
	case EqOp:
		return p.eq(doc)
	case LikeOp:
		return p.like(doc)
	case InOp:
		return p.in(doc)
	case GtOp, GtEqOp, LtOp, LtEqOp:
		return p.compare(doc)
	case ContainsOp:
		return p.contains(doc)
	case FunctionOp:
		return p.value.(func(*Document) bool)(doc)
	}
	return false
}

func (p *simplePredicate) exist(doc *Document) bool {
	return doc.Has(p.field)
}

func (p *simplePredicate) notExists(doc *Document) bool {
	return !p.exist(doc)
}

func (p *simplePredicate) compare(doc *Document) bool {
	normValue, err := encoding.Normalize(getFieldOrValue(doc, p.value))
	if err != nil {
		return false
	}

	res := compareValues(doc.Get(p.field), normValue)

	switch p.opType {
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

func (p *simplePredicate) eq(doc *Document) bool {
	normValue, err := encoding.Normalize(getFieldOrValue(doc, p.value))
	if err != nil {
		return false
	}

	if !doc.Has(p.field) {
		return false
	}

	return compareValues(doc.Get(p.field), normValue) == 0
}

func (p *simplePredicate) in(doc *Document) bool {
	values := p.value.([]interface{})

	docValue := doc.Get(p.field)
	for _, value := range values {
		normValue, err := encoding.Normalize(getFieldOrValue(doc, value))
		if err == nil && compareValues(normValue, docValue) == 0 {
			return true
		}
	}
	return false
}

func (p *simplePredicate) contains(doc *Document) bool {
	elems := p.value.([]interface{})

	fieldValue := doc.Get(p.field)
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
