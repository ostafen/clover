package encoding

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type TestStruct struct {
	IntField    int
	UintField   uint
	StringField string
	TimeField   time.Time
	SliceField  []int
	MapField    map[string]interface{}
}

func TestNormalize(t *testing.T) {
	date := time.Date(2020, 01, 1, 0, 0, 0, 0, time.UTC)

	s := &TestStruct{
		TimeField:  date,
		SliceField: []int{1, 2, 3, 4},
		MapField: map[string]interface{}{
			"hello": "clover",
		},
	}

	ns, err := Normalize(s)
	require.NoError(t, err)

	require.IsType(t, ns, map[string]interface{}{})

	s1 := &TestStruct{}
	err = Convert(ns.(map[string]interface{}), s1)
	require.NoError(t, err)

	require.Equal(t, s, s1)
}
