package clover

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCriteriaToQueryNode(t *testing.T) {
	c := Field("myField").Eq(1).And(Field("myField").Eq(2))
	node := toQueryNode(c)

	require.IsType(t, node, &binaryQueryNode{})

	binNode := node.(*binaryQueryNode)

	require.IsType(t, binNode.n1, &unaryQueryNode{})
	require.IsType(t, binNode.n2, &unaryQueryNode{})
}

func TestFlattenNot1(t *testing.T) {
	c := Field("myField").Gt(1).And(Field("myField").Lt(2)).Not()

	node := flattenNot(toQueryNode(c))

	binNode := node.(*binaryQueryNode)
	require.IsType(t, binNode.n1, &unaryQueryNode{})
	require.IsType(t, binNode.n2, &unaryQueryNode{})
}

func TestFlattenNot2(t *testing.T) {
	c := Field("myField").Eq(10).Not()

	node := flattenNot(toQueryNode(c))

	binNode := node.(*binaryQueryNode)
	require.IsType(t, binNode.n1, &unaryQueryNode{})
	require.IsType(t, binNode.n2, &unaryQueryNode{})

	n1 := binNode.n1.(*unaryQueryNode)
	n2 := binNode.n2.(*unaryQueryNode)

	require.Equal(t, n1.opType, LtOp)
	require.Equal(t, n2.opType, GtOp)
}

func TestMergeAndNodes(t *testing.T) {
	c := Field("myField").Gt(1).And(Field("myField").Lt(2))

	nd := flattenAndNodes(toQueryNode(c))
	require.IsType(t, &andQueryNode{}, nd)

	andNode := nd.(*andQueryNode)
	vRange, ok := andNode.fields["myField"]
	require.True(t, ok)

	require.Equal(t, int64(1), vRange.start)
	require.Equal(t, int64(2), vRange.end)

	require.False(t, vRange.includeStart)
	require.False(t, vRange.includeEnd)
}

func TestSelectIndexes(t *testing.T) {
	c := Field("a").Gt(1).And(Field("a").Lt(2)).Or(Field("b").Eq(100))

	nd := flattenAndNodes(flattenNot(toQueryNode(c)))

	require.Len(t, selectIndexes(nd, map[string]*indexImpl{
		"a": {
			fieldName: "a",
		},
	}), 0)

	require.Len(t, selectIndexes(nd, map[string]*indexImpl{
		"b": {
			fieldName: "b",
		},
	}), 0)

	indexes := selectIndexes(nd, map[string]*indexImpl{
		"a": {fieldName: "a"},
		"b": {fieldName: "b"},
	})

	require.Len(t, indexes, 2)

	require.Equal(t, indexes[0].index.fieldName, "a")
	require.Equal(t, indexes[1].index.fieldName, "b")
}
