package clover

import (
	"github.com/ostafen/clover/internal"
	"github.com/ostafen/clover/util"
)

type NotFlattenVisitor struct {
}

func (v *NotFlattenVisitor) VisitUnaryCriteria(c *UnaryCriteria) interface{} {
	return c
}

func (v *NotFlattenVisitor) VisitBinaryCriteria(c *BinaryCriteria) interface{} {
	return &BinaryCriteria{
		OpType: c.OpType,
		C1:     c.C1.Accept(v).(Criteria),
		C2:     c.C2.Accept(v).(Criteria),
	}
}

func (v *NotFlattenVisitor) VisitNotCriteria(c *NotCriteria) interface{} {
	switch criteriaType := c.C.(type) {
	case *UnaryCriteria:
		return v.removeNotCriteria(c)
	case *BinaryCriteria:
		// extract this into a separate method
		opType := criteriaType.OpType
		if opType == LogicalAnd {
			opType = LogicalOr
		} else {
			opType = LogicalAnd
		}

		return &BinaryCriteria{
			OpType: opType,
			C1:     v.VisitNotCriteria(&NotCriteria{criteriaType.C1}).(Criteria),
			C2:     v.VisitNotCriteria(&NotCriteria{criteriaType.C2}).(Criteria),
		}
	case *NotCriteria:
		return criteriaType.C
	}
	return c
}

func (v *NotFlattenVisitor) removeNotCriteria(c *NotCriteria) Criteria {
	innerNode := c.C

	unaryCriteria := innerNode.(*UnaryCriteria)

	switch unaryCriteria.OpType {
	case EqOp:
		return &BinaryCriteria{
			OpType: LogicalOr,
			C1: &UnaryCriteria{
				OpType: LtOp,
				Value:  unaryCriteria.Value,
				Field:  unaryCriteria.Field,
			},
			C2: &UnaryCriteria{
				OpType: GtOp,
				Field:  unaryCriteria.Field,
				Value:  unaryCriteria.Value,
			},
		}
	case LtOp:
		return &UnaryCriteria{
			OpType: GtEqOp,
			Value:  unaryCriteria.Value,
			Field:  unaryCriteria.Field,
		}
	case LtEqOp:
		return &UnaryCriteria{
			OpType: GtOp,
			Field:  unaryCriteria.Field,
			Value:  unaryCriteria.Value,
		}
	case GtOp:
		return &UnaryCriteria{
			OpType: LtEqOp,
			Value:  unaryCriteria.Value,
			Field:  unaryCriteria.Field,
		}
	case GtEqOp:
		return &UnaryCriteria{
			OpType: LtOp,
			Value:  unaryCriteria.Value,
			Field:  unaryCriteria.Field,
		}
	}

	return c
}

type IndexSelectVisitor struct {
	Fields map[string]bool
}

func (v *IndexSelectVisitor) VisitUnaryCriteria(c *UnaryCriteria) interface{} {
	if v.Fields[c.Field] {
		return []string{c.Field}
	}
	return []string{}
}

func (v *IndexSelectVisitor) VisitBinaryCriteria(c *BinaryCriteria) interface{} {
	leftIndexes := c.C1.Accept(v).([]string)
	rightIndexes := c.C2.Accept(v).([]string)

	if c.OpType == LogicalAnd { // select the indexes with the lowest number of queries
		if len(leftIndexes) > 0 && len(leftIndexes) < len(rightIndexes) {
			return leftIndexes
		}
		return rightIndexes
	}

	if len(leftIndexes) == 0 || len(rightIndexes) == 0 {
		return []string{}
	}

	res := make([]string, 0, len(leftIndexes)+len(rightIndexes))
	for _, field := range leftIndexes {
		res = append(res, field)
	}

	for _, field := range rightIndexes {
		res = append(res, field)
	}
	return res
}

func (v *IndexSelectVisitor) VisitNotCriteria(c *NotCriteria) interface{} {
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

func (v *FieldRangeVisitor) VisitUnaryCriteria(c *UnaryCriteria) interface{} {
	if v.Fields[c.Field] {
		return map[string]*valueRange{
			c.Field: unaryCriteriaToRange(c),
		}
	}
	return map[string]*valueRange{}
}

func (v *FieldRangeVisitor) VisitBinaryCriteria(c *BinaryCriteria) interface{} {
	leftRanges := c.C1.Accept(v).(map[string]*valueRange)
	rightRanges := c.C2.Accept(v).(map[string]*valueRange)

	mergedMap := make(map[string]*valueRange)
	for key, value := range leftRanges {
		mergedMap[key] = value
	}

	for key, value := range rightRanges {
		vRange := mergedMap[key]
		if vRange == nil {
			mergedMap[key] = value
		} else {
			mergedMap[key] = vRange.intersect(value)
		}
	}
	return mergedMap
}

func (v *FieldRangeVisitor) VisitNotCriteria(c *NotCriteria) interface{} {
	return c.C.Accept(v)
}

type CriteriaNormalizeVisitor struct {
	err error
}

func (v *CriteriaNormalizeVisitor) VisitUnaryCriteria(c *UnaryCriteria) interface{} {
	normValue := c.Value

	if c.OpType != FunctionOp {
		_, isField := c.Value.(*field)
		if !isField {
			var err error
			normValue, err = internal.Normalize(c.Value)
			if err != nil {
				v.err = err
				return nil
			}
		}
	}

	return &UnaryCriteria{
		Field:  c.Field,
		OpType: c.OpType,
		Value:  normValue,
	}
}

func (v *CriteriaNormalizeVisitor) VisitBinaryCriteria(c *BinaryCriteria) interface{} {
	leftRes := c.C1.Accept(v)
	rightRes := c.C2.Accept(v)

	if leftRes == nil || rightRes == nil {
		return nil
	}

	return &BinaryCriteria{
		OpType: c.OpType,
		C1:     leftRes.(Criteria),
		C2:     rightRes.(Criteria),
	}
}

func (v *CriteriaNormalizeVisitor) VisitNotCriteria(c *NotCriteria) interface{} {
	res := c.C.Accept(v)
	if res == nil {
		return nil
	}
	return &NotCriteria{C: res.(Criteria)}
}
