package clover

import (
	"github.com/ostafen/clover/v2/index"
	"github.com/ostafen/clover/v2/internal"
	"github.com/ostafen/clover/v2/query"
	"github.com/ostafen/clover/v2/util"
)

type NotFlattenVisitor struct {
}

func (v *NotFlattenVisitor) VisitUnaryCriteria(c *query.UnaryCriteria) interface{} {
	return c
}

func (v *NotFlattenVisitor) VisitBinaryCriteria(c *query.BinaryCriteria) interface{} {
	return &query.BinaryCriteria{
		OpType: c.OpType,
		C1:     c.C1.Accept(v).(query.Criteria),
		C2:     c.C2.Accept(v).(query.Criteria),
	}
}

func (v *NotFlattenVisitor) VisitNotCriteria(c *query.NotCriteria) interface{} {
	switch criteriaType := c.C.(type) {
	case *query.UnaryCriteria:
		return v.removeNotCriteria(c)
	case *query.BinaryCriteria:
		// extract this into a separate method
		opType := criteriaType.OpType
		if opType == query.LogicalAnd {
			opType = query.LogicalOr
		} else {
			opType = query.LogicalAnd
		}

		return &query.BinaryCriteria{
			OpType: opType,
			C1:     v.VisitNotCriteria(&query.NotCriteria{C: criteriaType.C1}).(query.Criteria),
			C2:     v.VisitNotCriteria(&query.NotCriteria{C: criteriaType.C2}).(query.Criteria),
		}
	case *query.NotCriteria:
		return criteriaType.C
	}
	return c
}

func (v *NotFlattenVisitor) removeNotCriteria(c *query.NotCriteria) query.Criteria {
	innerNode := c.C

	unaryCriteria := innerNode.(*query.UnaryCriteria)

	switch unaryCriteria.OpType {
	case query.EqOp:
		return &query.BinaryCriteria{
			OpType: query.LogicalOr,
			C1: &query.UnaryCriteria{
				OpType: query.LtOp,
				Value:  unaryCriteria.Value,
				Field:  unaryCriteria.Field,
			},
			C2: &query.UnaryCriteria{
				OpType: query.GtOp,
				Field:  unaryCriteria.Field,
				Value:  unaryCriteria.Value,
			},
		}
	case query.LtOp:
		return &query.UnaryCriteria{
			OpType: query.GtEqOp,
			Value:  unaryCriteria.Value,
			Field:  unaryCriteria.Field,
		}
	case query.LtEqOp:
		return &query.UnaryCriteria{
			OpType: query.GtOp,
			Field:  unaryCriteria.Field,
			Value:  unaryCriteria.Value,
		}
	case query.GtOp:
		return &query.UnaryCriteria{
			OpType: query.LtEqOp,
			Value:  unaryCriteria.Value,
			Field:  unaryCriteria.Field,
		}
	case query.GtEqOp:
		return &query.UnaryCriteria{
			OpType: query.LtOp,
			Value:  unaryCriteria.Value,
			Field:  unaryCriteria.Field,
		}
	}

	return c
}

type IndexSelectVisitor struct {
	Fields map[string]*index.IndexInfo
}

func (v *IndexSelectVisitor) VisitUnaryCriteria(c *query.UnaryCriteria) interface{} {
	info := v.Fields[c.Field]
	if info != nil {
		return []*index.IndexInfo{info}
	}
	return []*index.IndexInfo{}
}

func (v *IndexSelectVisitor) VisitBinaryCriteria(c *query.BinaryCriteria) interface{} {
	leftIndexes := c.C1.Accept(v).([]*index.IndexInfo)
	rightIndexes := c.C2.Accept(v).([]*index.IndexInfo)

	if c.OpType == query.LogicalAnd { // select the indexes with the lowest number of queries
		if len(leftIndexes) > 0 && len(leftIndexes) < len(rightIndexes) {
			return leftIndexes
		}
		return rightIndexes
	}

	if len(leftIndexes) == 0 || len(rightIndexes) == 0 {
		return []*index.IndexInfo{}
	}

	res := make([]*index.IndexInfo, 0, len(leftIndexes)+len(rightIndexes))
	res = append(res, leftIndexes...)
	res = append(res, rightIndexes...)
	return res
}

func (v *IndexSelectVisitor) VisitNotCriteria(c *query.NotCriteria) interface{} {
	return nil
}

type FieldRangeVisitor struct {
	Fields map[string]bool
}

func NewFieldRangeVisitor(fields []string) *FieldRangeVisitor {
	return &FieldRangeVisitor{
		Fields: util.StringSliceToSet(fields),
	}
}

func (v *FieldRangeVisitor) VisitUnaryCriteria(c *query.UnaryCriteria) interface{} {
	if v.Fields[c.Field] {
		r := unaryCriteriaToRange(c)
		if r != nil {
			return map[string]*index.Range{c.Field: r}
		}
	}
	return map[string]*index.Range{}
}

func (v *FieldRangeVisitor) VisitBinaryCriteria(c *query.BinaryCriteria) interface{} {
	leftRanges := c.C1.Accept(v).(map[string]*index.Range)
	rightRanges := c.C2.Accept(v).(map[string]*index.Range)

	mergedMap := make(map[string]*index.Range)
	for key, value := range leftRanges {
		mergedMap[key] = value
	}

	for key, value := range rightRanges {
		vRange := mergedMap[key]
		if vRange == nil {
			mergedMap[key] = value
		} else {
			mergedMap[key] = vRange.Intersect(value)
		}
	}
	return mergedMap
}

func (v *FieldRangeVisitor) VisitNotCriteria(c *query.NotCriteria) interface{} {
	return c.C.Accept(v)
}

type CriteriaNormalizeVisitor struct {
	err error
}

func (v *CriteriaNormalizeVisitor) VisitUnaryCriteria(c *query.UnaryCriteria) interface{} {
	normValue := c.Value

	if c.OpType != query.FunctionOp {
		if !query.IsField(c.Value) {
			var err error
			normValue, err = internal.Normalize(c.Value)
			if err != nil {
				v.err = err
				return nil
			}
		}
	}

	return &query.UnaryCriteria{
		Field:  c.Field,
		OpType: c.OpType,
		Value:  normValue,
	}
}

func (v *CriteriaNormalizeVisitor) VisitBinaryCriteria(c *query.BinaryCriteria) interface{} {
	leftRes := c.C1.Accept(v)
	rightRes := c.C2.Accept(v)

	if leftRes == nil || rightRes == nil {
		return nil
	}

	return &query.BinaryCriteria{
		OpType: c.OpType,
		C1:     leftRes.(query.Criteria),
		C2:     rightRes.(query.Criteria),
	}
}

func (v *CriteriaNormalizeVisitor) VisitNotCriteria(c *query.NotCriteria) interface{} {
	res := c.C.Accept(v)
	if res == nil {
		return nil
	}
	return &query.NotCriteria{C: res.(query.Criteria)}
}

func unaryCriteriaToRange(c *query.UnaryCriteria) *index.Range {
	switch c.OpType {
	case query.EqOp:
		return &index.Range{
			Start:         c.Value,
			End:           c.Value,
			StartIncluded: true,
			EndIncluded:   true,
		}
	case query.LtOp:
		return &index.Range{
			Start:         nil,
			End:           c.Value,
			StartIncluded: false,
			EndIncluded:   false,
		}
	case query.LtEqOp:
		return &index.Range{
			Start:         nil,
			End:           c.Value,
			StartIncluded: false,
			EndIncluded:   true,
		}
	case query.GtOp:
		return &index.Range{
			Start:         c.Value,
			End:           nil,
			StartIncluded: false,
			EndIncluded:   false,
		}
	case query.GtEqOp:
		return &index.Range{
			Start:         c.Value,
			End:           nil,
			StartIncluded: true,
			EndIncluded:   false,
		}
	}
	return nil
}
