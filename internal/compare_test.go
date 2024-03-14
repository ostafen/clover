package internal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCompareDifferentTypes(t *testing.T) {
	require.Negative(t, Compare(nil, uint64(10)))
	require.Negative(t, Compare(nil, 10.10))
	require.Negative(t, Compare(nil, true))
	require.Negative(t, Compare(nil, "10"))

	require.Negative(t, Compare(uint64(10), map[string]interface{}{}))
	require.Negative(t, Compare(int64(10), "10"))

	require.Negative(t, Compare([]interface{}{}, time.Time{}))

	require.Negative(t, Compare(uint64(10), []interface{}{}))
}

func TestCompareNumbers(t *testing.T) {
	require.Zero(t, Compare(uint64(10), int64(10)))
	require.Zero(t, Compare(uint64(10), 10.0))
	require.Zero(t, Compare(int64(10), 10.0))
}

func TestCompareBooleans(t *testing.T) {
	require.Zero(t, Compare(true, true))
	require.Negative(t, Compare(false, true))
	require.Positive(t, Compare(true, false))
}

func TestCompareStrings(t *testing.T) {
	require.Zero(t, Compare("clover", "clover"))
	require.Negative(t, Compare("c", "clover"))
	require.Positive(t, Compare("clover", "c"))
}

func TestCompareTimes(t *testing.T) {
	require.Negative(t, Compare(time.Now(), time.Now().Add(time.Second)))
}

func TestCompareSlices(t *testing.T) {
	s := []interface{}{10.0, map[string]interface{}{"hello": "clover"}, "clover", true, []interface{}{}}
	require.Zero(t, Compare(s, s))
}

func TestCompareObjects(t *testing.T) {
	a := map[string]interface{}{
		"SomeNumber": float64(0),
		"SomeString": "aString",
	}
	require.Zero(t, Compare(a, a))

	b := map[string]interface{}{
		"data": map[string]interface{}{
			"SomeNumber": float64(0),
			"SomeString": "aString",
		},
	}

	c := map[string]interface{}{
		"data": map[string]interface{}{
			"SomeNumber": float64(0),
			"SomeString": "aStr",
		},
	}

	d := map[string]interface{}{
		"data": map[string]interface{}{
			"SomeNumber": float64(0),
			"SomeString": "aStr",
		},
	}

	require.Positive(t, Compare(b, c))
	require.Positive(t, Compare(b, d))
}
