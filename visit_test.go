package clover

import (
	"testing"

	"github.com/ostafen/clover/v2/index"
	q "github.com/ostafen/clover/v2/query"
	"github.com/stretchr/testify/require"
)

func TestFlattenNot1(t *testing.T) {
	c := q.Field("myField").Gt(1).And(q.Field("myField").Lt(2)).Not()
	c = c.Accept(&CriteriaNormalizeVisitor{}).(q.Criteria)
	c = c.Accept(&NotFlattenVisitor{}).(q.Criteria)

	binNode := c.(*q.BinaryCriteria)
	require.Equal(t, binNode.OpType, q.LogicalOr)

	require.IsType(t, binNode.C1, &q.UnaryCriteria{})
	require.IsType(t, binNode.C2, &q.UnaryCriteria{})

	require.Equal(t, binNode.C1.(*q.UnaryCriteria).OpType, q.LtEqOp)
	require.Equal(t, binNode.C2.(*q.UnaryCriteria).OpType, q.GtEqOp)
}

func TestFlattenNot2(t *testing.T) {
	c := q.Field("myField").Eq(10).Not()

	c = c.Accept(&CriteriaNormalizeVisitor{}).(q.Criteria)
	c = c.Accept(&NotFlattenVisitor{}).(q.Criteria)

	binNode := c.(*q.BinaryCriteria)
	require.Equal(t, binNode.OpType, q.LogicalOr)
	require.IsType(t, binNode.C1, &q.UnaryCriteria{})
	require.IsType(t, binNode.C2, &q.UnaryCriteria{})

	c1 := binNode.C1.(*q.UnaryCriteria)
	c2 := binNode.C2.(*q.UnaryCriteria)

	require.Equal(t, c1.OpType, q.LtOp)
	require.Equal(t, c2.OpType, q.GtOp)
}

func TestFlattenNot3(t *testing.T) {
	c := q.Field("myField").GtEq(10).Or(q.Field("myField").LtEq(100)).Not()

	c = c.Accept(&CriteriaNormalizeVisitor{}).(q.Criteria)
	c = c.Accept(&NotFlattenVisitor{}).(q.Criteria)

	binNode := c.(*q.BinaryCriteria)
	require.Equal(t, binNode.OpType, q.LogicalAnd)
	require.IsType(t, binNode.C1, &q.UnaryCriteria{})
	require.IsType(t, binNode.C2, &q.UnaryCriteria{})

	c1 := binNode.C1.(*q.UnaryCriteria)
	c2 := binNode.C2.(*q.UnaryCriteria)

	require.Equal(t, c1.OpType, q.LtOp)
	require.Equal(t, c2.OpType, q.GtOp)
}

func TestSelectIndexes(t *testing.T) {
	c := q.Field("a").Gt(1).And(q.Field("a").Lt(2)).Or(q.Field("b").Eq(100))

	c = c.Accept(&CriteriaNormalizeVisitor{}).(q.Criteria)
	c = c.Accept(&NotFlattenVisitor{}).(q.Criteria)

	s := c.Accept(&IndexSelectVisitor{Fields: map[string]*index.IndexInfo{
		"a": {Field: "a"},
	}}).([]*index.IndexInfo)
	require.Len(t, s, 0)

	s = c.Accept(&IndexSelectVisitor{Fields: map[string]*index.IndexInfo{
		"b": {Field: "b"},
	}}).([]*index.IndexInfo)
	require.Len(t, s, 0)

	s = c.Accept(&IndexSelectVisitor{Fields: map[string]*index.IndexInfo{
		"a": {Field: "a"},
		"b": {Field: "b"},
	}}).([]*index.IndexInfo)

	require.Len(t, s, 2)

	require.Equal(t, s[0], &index.IndexInfo{Field: "a"})
	require.Equal(t, s[1], &index.IndexInfo{Field: "b"})
}
