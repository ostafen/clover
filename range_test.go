package clover

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRangeIsEmpty(t *testing.T) {
	r := &valueRange{start: uint64(10), end: uint64(9), startIncluded: true, endIncluded: true}
	require.True(t, r.isEmpty())

	r = &valueRange{start: uint64(10), end: uint64(10)}
	require.True(t, r.isEmpty())
}

func TestRangeIntersect(t *testing.T) {
	r1 := &valueRange{start: uint64(10), end: uint64(100), startIncluded: true, endIncluded: true}
	r2 := &valueRange{start: uint64(20), end: uint64(90), startIncluded: true, endIncluded: true}
	require.Equal(t, r1.intersect(r2), r2)
	require.Equal(t, r2.intersect(r1), r2)

	r1 = &valueRange{start: uint64(10), end: uint64(60), startIncluded: true, endIncluded: true}
	r2 = &valueRange{start: uint64(50), end: uint64(100), startIncluded: true, endIncluded: true}
	require.Equal(t, r1.intersect(r2), &valueRange{start: uint64(50), end: uint64(60), startIncluded: true, endIncluded: true})
	require.Equal(t, r2.intersect(r1), &valueRange{start: uint64(50), end: uint64(60), startIncluded: true, endIncluded: true})
}
