package index

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRangeIsEmpty(t *testing.T) {
	r := &Range{Start: uint64(10), End: uint64(9), StartIncluded: true, EndIncluded: true}
	require.True(t, r.IsEmpty())

	r = &Range{Start: uint64(10), End: uint64(10)}
	require.True(t, r.IsEmpty())

	r = &Range{Start: uint64(10), End: nil}
	require.False(t, r.IsEmpty())

	r = &Range{Start: nil, End: uint64(10)}
	require.False(t, r.IsEmpty())
}

func TestRangeIntersect(t *testing.T) {
	r1 := &Range{Start: uint64(10), End: uint64(100), StartIncluded: true, EndIncluded: true}
	r2 := &Range{Start: uint64(20), End: uint64(90), StartIncluded: true, EndIncluded: true}
	require.Equal(t, r1.Intersect(r2), r2)
	require.Equal(t, r2.Intersect(r1), r2)

	r1 = &Range{Start: uint64(10), End: uint64(60), StartIncluded: true, EndIncluded: true}
	r2 = &Range{Start: uint64(50), End: uint64(100), StartIncluded: true, EndIncluded: true}
	require.Equal(t, r1.Intersect(r2), &Range{Start: uint64(50), End: uint64(60), StartIncluded: true, EndIncluded: true})
	require.Equal(t, r2.Intersect(r1), &Range{Start: uint64(50), End: uint64(60), StartIncluded: true, EndIncluded: true})
}
