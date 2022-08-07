package query

import (
	"regexp"
	"strings"

	d "github.com/ostafen/clover/v2/document"
	"github.com/ostafen/clover/v2/internal"
)

const (
	ExistsOp = iota
	EqOp
	NeqOp
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
	Satisfy(doc *d.Document) bool
	Accept(v CriteriaVisitor) interface{}
	Not() Criteria
	And(c Criteria) Criteria
	Or(c Criteria) Criteria
}

type BinaryCriteria struct {
	OpType int
	C1, C2 Criteria
}

func (c *BinaryCriteria) Accept(v CriteriaVisitor) interface{} {
	return v.VisitBinaryCriteria(c)
}

type NotCriteria struct {
	C Criteria
}

func (c *NotCriteria) Not() Criteria {
	return not(c)
}

func (c *NotCriteria) And(other Criteria) Criteria {
	return and(c, other)
}

func (c *NotCriteria) Or(other Criteria) Criteria {
	return or(c, other)
}

func (c *NotCriteria) Satisfy(doc *d.Document) bool {
	return !c.C.Satisfy(doc)
}

func (c *NotCriteria) Accept(v CriteriaVisitor) interface{} {
	return v.VisitNotCriteria(c)
}

func (c *BinaryCriteria) Not() Criteria {
	return not(c)
}

func (c *BinaryCriteria) And(other Criteria) Criteria {
	return and(c, other)
}

func (c *BinaryCriteria) Or(other Criteria) Criteria {
	return or(c, other)
}

func (c *BinaryCriteria) Satisfy(doc *d.Document) bool {
	if c.OpType == LogicalAnd {
		return c.C1.Satisfy(doc) && c.C2.Satisfy(doc)
	}
	return c.C1.Satisfy(doc) || c.C2.Satisfy(doc)
}

type UnaryCriteria struct {
	OpType int
	Field  string
	Value  interface{}
}

func (c *UnaryCriteria) Not() Criteria {
	return not(c)
}

func (c *UnaryCriteria) And(other Criteria) Criteria {
	return and(c, other)
}

func (c *UnaryCriteria) Or(other Criteria) Criteria {
	return or(c, other)
}

func (c *UnaryCriteria) Satisfy(doc *d.Document) bool {
	switch c.OpType {
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
		return c.Value.(func(*d.Document) bool)(doc)
	}
	return false
}

func (c *UnaryCriteria) Accept(v CriteriaVisitor) interface{} {
	return v.VisitUnaryCriteria(c)
}

func and(c1, c2 Criteria) Criteria {
	return &BinaryCriteria{
		OpType: LogicalAnd,
		C1:     c1,
		C2:     c2,
	}
}

func or(c1, c2 Criteria) Criteria {
	return &BinaryCriteria{
		OpType: LogicalOr,
		C1:     c1,
		C2:     c2,
	}
}

func not(c Criteria) Criteria {
	return &NotCriteria{c}
}

func newCriteria(opType int, field string, value interface{}) Criteria {
	return &UnaryCriteria{
		OpType: opType,
		Field:  field,
		Value:  value,
	}
}

type field struct {
	name string
}

func IsField(v interface{}) bool {
	_, ok := v.(*field)
	return ok
}

// Field represents a document field. It is used to create a new criteria.
func Field(name string) *field {
	return &field{name: name}
}

func (f *field) Exists() Criteria {
	return newCriteria(ExistsOp, f.name, nil)
}

func (f *field) NotExists() Criteria {
	return newCriteria(ExistsOp, f.name, nil).Not()
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
	return newCriteria(EqOp, f.name, value)
}

func (f *field) Gt(value interface{}) Criteria {
	return newCriteria(GtOp, f.name, value)
}

func (f *field) GtEq(value interface{}) Criteria {
	return newCriteria(GtEqOp, f.name, value)
}

func (f *field) Lt(value interface{}) Criteria {
	return newCriteria(LtOp, f.name, value)
}

func (f *field) LtEq(value interface{}) Criteria {
	return newCriteria(LtEqOp, f.name, value)
}

func (f *field) Neq(value interface{}) Criteria {
	return f.Eq(value).Not()
}

func (f *field) In(values ...interface{}) Criteria {
	return newCriteria(InOp, f.name, values)
}

func (f *field) Like(pattern string) Criteria {
	return newCriteria(LikeOp, f.name, pattern)
}

func (f *field) Contains(elems ...interface{}) Criteria {
	return newCriteria(ContainsOp, f.name, elems)
}

// getFieldOrValue returns dereferenced value if value denotes another document field,
// otherwise returns the value itself directly
func getFieldOrValue(doc *d.Document, value interface{}) interface{} {
	if cmpField, ok := value.(*field); ok {
		value = doc.Get(cmpField.name)
	} else if fStr, ok := value.(string); ok && strings.HasPrefix(fStr, "$") {
		fieldName := strings.TrimLeft(fStr, "$")
		value = doc.Get(fieldName)
	}
	return value
}

func (c *UnaryCriteria) compare(doc *d.Document) bool {
	normValue, err := internal.Normalize(getFieldOrValue(doc, c.Value))
	if err != nil {
		return false
	}

	res := internal.Compare(doc.Get(c.Field), normValue)

	switch c.OpType {
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

func (c *UnaryCriteria) exist(doc *d.Document) bool {
	return doc.Has(c.Field)
}

func (c *UnaryCriteria) eq(doc *d.Document) bool {
	value := getFieldOrValue(doc, c.Value)

	if !doc.Has(c.Field) {
		return false
	}

	return internal.Compare(doc.Get(c.Field), value) == 0
}

func (c *UnaryCriteria) in(doc *d.Document) bool {
	values := c.Value.([]interface{})

	docValue := doc.Get(c.Field)
	for _, value := range values {
		actualValue := getFieldOrValue(doc, value)
		if internal.Compare(actualValue, docValue) == 0 {
			return true
		}
	}
	return false
}

func (c *UnaryCriteria) contains(doc *d.Document) bool {
	elems := c.Value.([]interface{})

	fieldValue := doc.Get(c.Field)
	slice, _ := fieldValue.([]interface{})

	if fieldValue == nil || slice == nil {
		return false
	}

	for _, elem := range elems {
		found := false
		actualValue := getFieldOrValue(doc, elem)

		for _, val := range slice {
			if internal.Compare(actualValue, val) == 0 {
				found = true
				break
			}
		}

		if !found {
			return false
		}

	}
	return true
}

func (c *UnaryCriteria) like(doc *d.Document) bool {
	pattern := c.Value.(string)

	s, isString := doc.Get(c.Field).(string)
	if !isString {
		return false
	}
	matched, err := regexp.MatchString(pattern, s)
	return matched && err == nil
}

type CriteriaVisitor interface {
	VisitUnaryCriteria(c *UnaryCriteria) interface{}
	VisitNotCriteria(c *NotCriteria) interface{}
	VisitBinaryCriteria(c *BinaryCriteria) interface{}
}
