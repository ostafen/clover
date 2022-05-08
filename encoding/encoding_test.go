package encoding

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type TestStruct struct {
	IntField    int  `clover:"int"`
	UintField   uint `clover:"uint,omitempty"`
	StringField string
	FloatField  float32
	BoolField   bool
	TimeField   time.Time
	SliceField  []int
	MapField    map[string]interface{}
	Data        []byte
}

func TestNormalize(t *testing.T) {
	date := time.Date(2020, 01, 1, 0, 0, 0, 0, time.UTC)

	s := &TestStruct{
		TimeField:  date,
		IntField:   10,
		FloatField: 0.1,
		BoolField:  true,
		SliceField: []int{1, 2, 3, 4},
		Data:       []byte("hello, clover!"),
		MapField: map[string]interface{}{
			"hello": "clover",
		},
	}

	ns, err := Normalize(s)
	require.NoError(t, err)

	require.IsType(t, ns, map[string]interface{}{})
	m := ns.(map[string]interface{})
	require.Nil(t, m["uint"]) // testing omitempty

	require.IsType(t, m["Data"], []byte{})

	s1 := &TestStruct{}
	err = Convert(ns.(map[string]interface{}), s1)
	require.NoError(t, err)

	require.Equal(t, s, s1)
}

func TestNormalize2(t *testing.T) {
	norm, err := Normalize(nil)
	require.NoError(t, err)
	require.Nil(t, norm)

	_, err = Normalize(make(chan struct{}))
	require.Error(t, err)
}

func TestEncodeDecode(t *testing.T) {
	date := time.Date(2020, 01, 1, 0, 0, 0, 0, time.UTC)

	s := &TestStruct{
		TimeField:  date,
		SliceField: []int{1, 2, 3, 4},
		MapField: map[string]interface{}{
			"hello": "clover",
		},
	}

	data, err := Encode(s)
	require.NoError(t, err)

	s1 := &TestStruct{}
	require.NoError(t, Decode(data, s1))

	require.Equal(t, s, s1)
}
