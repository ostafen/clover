package clover

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFlattenNot1(t *testing.T) {
	c := Field("myField").Gt(1).And(Field("myField").Lt(2)).Not()
	c = c.Accept(&CriteriaNormalizeVisitor{}).(Criteria)
	c = c.Accept(&NotFlattenVisitor{}).(Criteria)

	binNode := c.(*BinaryCriteria)
	require.IsType(t, binNode.C1, &UnaryCriteria{})
	require.IsType(t, binNode.C2, &UnaryCriteria{})
}

func TestFlattenNot2(t *testing.T) {
	c := Field("myField").Eq(10).Not()

	c = c.Accept(&CriteriaNormalizeVisitor{}).(Criteria)
	c = c.Accept(&NotFlattenVisitor{}).(Criteria)

	binNode := c.(*BinaryCriteria)
	require.IsType(t, binNode.C1, &UnaryCriteria{})
	require.IsType(t, binNode.C2, &UnaryCriteria{})

	c1 := binNode.C1.(*UnaryCriteria)
	c2 := binNode.C2.(*UnaryCriteria)

	require.Equal(t, c1.OpType, LtOp)
	require.Equal(t, c2.OpType, GtOp)
}

func TestSelectIndexes(t *testing.T) {
	c := Field("a").Gt(1).And(Field("a").Lt(2)).Or(Field("b").Eq(100))

	c = c.Accept(&CriteriaNormalizeVisitor{}).(Criteria)
	c = c.Accept(&NotFlattenVisitor{}).(Criteria)

	s := c.Accept(&IndexSelectVisitor{Fields: map[string]bool{
		"a": true,
	}}).([]string)
	require.Len(t, s, 0)

	s = c.Accept(&IndexSelectVisitor{Fields: map[string]bool{
		"b": true,
	}}).([]string)
	require.Len(t, s, 0)

	s = c.Accept(&IndexSelectVisitor{Fields: map[string]bool{
		"a": true,
		"b": true,
	}}).([]string)

	require.Len(t, s, 2)

	require.Equal(t, s[0], "a")
	require.Equal(t, s[1], "b")
}
