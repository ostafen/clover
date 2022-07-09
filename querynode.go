package clover

import (
	"regexp"
	"strings"

	"github.com/ostafen/clover/encoding"
)

type queryNode interface {
	Satisfy(doc *Document) bool
}

type notQueryNode struct {
	queryNode
}

func (nd *notQueryNode) Satisfy(doc *Document) bool {
	return !nd.queryNode.Satisfy(doc)
}

type unaryQueryNode struct {
	opType int
	field  string
	value  interface{}
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

func (nd *unaryQueryNode) Satisfy(doc *Document) bool {
	switch nd.opType {
	case ExistsOp:
		return nd.exist(doc)
	case EqOp:
		return nd.eq(doc)
	case LikeOp:
		return nd.like(doc)
	case InOp:
		return nd.in(doc)
	case GtOp, GtEqOp, LtOp, LtEqOp:
		return nd.compare(doc)
	case ContainsOp:
		return nd.contains(doc)
	case FunctionOp:
		return nd.value.(func(*Document) bool)(doc)
	}
	return false
}

func (nd *unaryQueryNode) compare(doc *Document) bool {
	normValue, err := encoding.Normalize(getFieldOrValue(doc, nd.value))
	if err != nil {
		return false
	}

	res := encoding.Compare(doc.Get(nd.field), normValue)

	switch nd.opType {
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

func (nd *unaryQueryNode) exist(doc *Document) bool {
	return doc.Has(nd.field)
}

func (nd *unaryQueryNode) eq(doc *Document) bool {
	value := getFieldOrValue(doc, nd.value)

	if !doc.Has(nd.field) {
		return false
	}

	return encoding.Compare(doc.Get(nd.field), value) == 0
}

func (nd *unaryQueryNode) in(doc *Document) bool {
	values := nd.value.([]interface{})

	docValue := doc.Get(nd.field)
	for _, value := range values {
		actualValue := getFieldOrValue(doc, value)
		if encoding.Compare(actualValue, docValue) == 0 {
			return true
		}
	}
	return false
}

func (nd *unaryQueryNode) contains(doc *Document) bool {
	elems := nd.value.([]interface{})

	fieldValue := doc.Get(nd.field)
	slice, _ := fieldValue.([]interface{})

	if fieldValue == nil || slice == nil {
		return false
	}

	for _, elem := range elems {
		found := false
		actualValue := getFieldOrValue(doc, elem)

		for _, val := range slice {
			if encoding.Compare(actualValue, val) == 0 {
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

func (nd *unaryQueryNode) like(doc *Document) bool {
	pattern := nd.value.(string)

	s, isString := doc.Get(nd.field).(string)
	if !isString {
		return false
	}
	matched, err := regexp.MatchString(pattern, s)
	return matched && err == nil
}

func newUnaryQueryNode(c *SimpleCriteria) *unaryQueryNode {
	normValue := c.Value

	if c.OpType != FunctionOp {
		_, isField := c.Value.(*field)
		if !isField {
			var err error
			normValue, err = encoding.Normalize(c.Value)
			if err != nil {
				return nil
			}
		}
	}

	return &unaryQueryNode{
		field:  c.Field,
		opType: c.OpType,
		value:  normValue,
	}
}

type binaryQueryNode struct {
	OpType int
	n1, n2 queryNode
}

func (nd *binaryQueryNode) Satisfy(doc *Document) bool {
	if nd.OpType == LogicalAnd {
		return nd.n1.Satisfy(doc) && nd.n2.Satisfy(doc)
	}
	return nd.n1.Satisfy(doc) || nd.n2.Satisfy(doc)
}

type emptyQueryNode struct {
	value bool
}

func (nd *emptyQueryNode) Satisfy(_ *Document) bool {
	return nd.value
}

func toQueryNode(c Criteria) queryNode {
	switch cType := c.(type) {
	case *SimpleCriteria:
		return toUnaryNode(cType)
	case *NotCriteria:
		return toNotNode(cType)
	case *BinaryCriteria:
		return toBinaryNode(cType)
	case nil:
		return &emptyQueryNode{true}
	}
	panic("unspected criteria")
}

func toUnaryNode(c *SimpleCriteria) queryNode {
	nd := newUnaryQueryNode(c)
	if nd == nil {
		return &emptyQueryNode{false}
	}
	return nd
}

func toNotNode(c *NotCriteria) *notQueryNode {
	return &notQueryNode{toQueryNode(c.C)}
}

func toBinaryNode(c *BinaryCriteria) *binaryQueryNode {
	return &binaryQueryNode{
		OpType: c.OpType,
		n1:     toQueryNode(c.C1),
		n2:     toQueryNode(c.C2),
	}
}

func tryToRemoveNot(nd *notQueryNode) queryNode {
	innerNode := nd.queryNode

	unaryNode := innerNode.(*unaryQueryNode)
	if unaryNode == nil {
		return nd
	}

	switch unaryNode.opType {
	case EqOp:
		return &binaryQueryNode{
			OpType: LogicalOr,
			n1: &unaryQueryNode{
				opType: LtOp,
				value:  unaryNode.value,
				field:  unaryNode.field,
			},
			n2: &unaryQueryNode{
				opType: GtOp,
				value:  unaryNode.value,
				field:  unaryNode.field,
			},
		}
	case LtOp:
		return &unaryQueryNode{
			opType: GtEqOp,
			value:  unaryNode.value,
			field:  unaryNode.field,
		}
	case LtEqOp:
		return &unaryQueryNode{
			opType: GtOp,
			value:  unaryNode.value,
			field:  unaryNode.field,
		}
	case GtOp:
		return &unaryQueryNode{
			opType: LtEqOp,
			value:  unaryNode.value,
			field:  unaryNode.field,
		}
	case GtEqOp:
		return &unaryQueryNode{
			opType: LtOp,
			value:  unaryNode.value,
			field:  unaryNode.field,
		}
	}

	return nd
}

func flattenNot(node queryNode) queryNode {
	notNd, _ := node.(*notQueryNode)
	if notNd != nil {
		innerNode := notNd.queryNode
		switch ndType := innerNode.(type) {
		case *binaryQueryNode:
			n1 := flattenNot(&notQueryNode{ndType.n1})
			n2 := flattenNot(&notQueryNode{ndType.n2})
			return &binaryQueryNode{ndType.OpType, n1, n2}
		case *unaryQueryNode:
			return tryToRemoveNot(notNd)
		}
	}

	switch ndType := node.(type) {
	case *binaryQueryNode:
		n1 := flattenNot(ndType.n1)
		n2 := flattenNot(ndType.n2)
		return &binaryQueryNode{ndType.OpType, n1, n2}
	}

	return node
}

type valueRange struct {
	start, end               interface{}
	includeStart, includeEnd bool
}

func unaryNodeToRange(nd *unaryQueryNode) *valueRange {
	switch nd.opType {
	case EqOp:
		return &valueRange{
			start:        nd.value,
			end:          nd.value,
			includeStart: true,
			includeEnd:   true,
		}
	case LtOp:
		return &valueRange{
			start:        nil,
			end:          nd.value,
			includeStart: false,
			includeEnd:   false,
		}
	case LtEqOp:
		return &valueRange{
			start:        nil,
			end:          nd.value,
			includeStart: false,
			includeEnd:   true,
		}
	case GtOp:
		return &valueRange{
			start:        nd.value,
			end:          nil,
			includeStart: false,
			includeEnd:   false,
		}
	case GtEqOp:
		return &valueRange{
			start:        nd.value,
			end:          nil,
			includeStart: true,
			includeEnd:   false,
		}
	}
	return nil
}

func (r *valueRange) isSingleType() bool {
	return true
}

func (r1 *valueRange) intersect(r2 *valueRange) *valueRange {
	intersection := &valueRange{
		start:        r1.start,
		end:          r1.end,
		includeStart: r1.includeStart,
		includeEnd:   r1.includeEnd,
	}

	res := encoding.Compare(r2.start, intersection.start)
	if res > 0 {
		intersection.start = r2.start
		intersection.includeStart = r2.includeStart
	} else if res == 0 {
		intersection.includeStart = intersection.includeStart && r2.includeStart
	} else if intersection.start == nil {
		intersection.start = r2.start
		intersection.includeStart = r2.includeStart
	}

	res = encoding.Compare(r2.end, intersection.end)
	if res < 0 {
		intersection.end = r2.end
		intersection.includeEnd = r2.includeEnd
	} else if res == 0 {
		intersection.includeEnd = intersection.includeEnd && r2.includeEnd
	} else if intersection.end == nil {
		intersection.end = r2.end
		intersection.includeEnd = r2.includeEnd
	}
	return intersection
}

type andQueryNode struct {
	fields map[string]*valueRange
}

func (nd *andQueryNode) Satisfy(doc *Document) bool {
	panic("not allowed")
}

func unaryToAndQueryNode(nd *unaryQueryNode) *andQueryNode {
	return &andQueryNode{
		fields: map[string]*valueRange{
			nd.field: unaryNodeToRange(nd),
		},
	}
}

func mergeAndNodes(n1 *andQueryNode, n2 *andQueryNode) *andQueryNode {
	merged := &andQueryNode{
		fields: make(map[string]*valueRange),
	}

	for key, value := range n1.fields {
		merged.fields[key] = value
	}

	for key, value := range n2.fields {
		if merged.fields[key] != nil {
			merged.fields[key] = merged.fields[key].intersect(value)
		} else {
			merged.fields[key] = value
		}
	}

	return merged
}

func flattenAndNodes(node queryNode) queryNode {
	switch ndType := node.(type) {
	case *notQueryNode:
		return &notQueryNode{flattenAndNodes(ndType.queryNode)}
	case *binaryQueryNode:
		n1 := flattenAndNodes(ndType.n1)
		n2 := flattenAndNodes(ndType.n2)

		if ndType.OpType == LogicalAnd {
			n1AndNode, _ := n1.(*andQueryNode)
			n2AndNode, _ := n2.(*andQueryNode)
			if n1AndNode != nil && n2AndNode != nil {
				return mergeAndNodes(n1AndNode, n2AndNode)
			}
		}

		return &binaryQueryNode{
			OpType: ndType.OpType,
			n1:     n1,
			n2:     n2,
		}

	case *unaryQueryNode:
		return unaryToAndQueryNode(ndType)
	}
	return node
}
