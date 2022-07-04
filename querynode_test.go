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
