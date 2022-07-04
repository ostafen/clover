package clover

const (
	objectIdField = "_id"
)

type predicate func(doc *Document) bool

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

type SimpleCriteria struct {
	OpType int
	Field  string
	Value  interface{}
}

func (c *SimpleCriteria) Not() Criteria {
	return not(c)
}

func (c *SimpleCriteria) And(other Criteria) Criteria {
	return and(c, other)
}

func (c *SimpleCriteria) Or(other Criteria) Criteria {
	return or(c, other)
}

func (c *SimpleCriteria) Accept(v CriteriaVisitor) interface{} {
	return v.VisitSimpleCriteria(c)
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

func newCriterion(opType int, field string, value interface{}) Criteria {
	return &SimpleCriteria{
		OpType: opType,
		Field:  field,
		Value:  value,
	}
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

type CriteriaVisitor interface {
	VisitSimpleCriteria(c *SimpleCriteria) interface{}
	VisitNotCriteria(c *NotCriteria) interface{}
	VisitBinaryCriteria(c *BinaryCriteria) interface{}
}
