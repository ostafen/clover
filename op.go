package clover

import (
	d "github.com/ostafen/clover/v2/document"
	"github.com/ostafen/clover/v2/util"
)

type UpdateOp interface {
	Apply(doc *d.Document) *d.Document
}

type SetOp struct {
	Field string
	Value interface{}
}

func (op *SetOp) Apply(doc *d.Document) *d.Document {
	doc.Set(op.Field, op.Value)
	return doc
}

func Set(field string, value interface{}) UpdateOp {
	return &SetOp{Field: field, Value: value}
}

type UnsetOp struct {
	Field string
}

func (op *UnsetOp) Apply(doc *d.Document) *d.Document {
	doc.Remove(op.Field)
	return doc
}

func Unset(field string) UpdateOp {
	return &UnsetOp{Field: field}
}

type IncOp struct {
	Field string
	Value interface{}
}

func (op *IncOp) Apply(doc *d.Document) *d.Document {
	val := doc.Get(op.Field)
	if util.IsNumber(val) && util.IsNumber(op.Value) {
		doc.Set(op.Field, util.ToFloat64(val)+util.ToFloat64(op.Value))
	}
	return doc
}

func Inc(field string, value interface{}) UpdateOp {
	return &IncOp{Field: field, Value: value}
}

type PushOp struct {
	Field string
	Value interface{}
}

func (op *PushOp) Apply(doc *d.Document) *d.Document {
	val := doc.Get(op.Field)
	if val == nil {
		doc.Set(op.Field, []interface{}{op.Value})
	} else if s, ok := val.([]interface{}); ok {
		doc.Set(op.Field, append(s, op.Value))
	}
	return doc
}

func Push(field string, value interface{}) UpdateOp {
	return &PushOp{Field: field, Value: value}
}
