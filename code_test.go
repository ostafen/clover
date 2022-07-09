package clover

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/ostafen/clover/encoding"
	"github.com/stretchr/testify/require"
)

func getSign(v int) int {
	if v > 0 {
		return 1
	} else if v < 0 {
		return -1
	}
	return 0
}

func TestEncodeObject(t *testing.T) {
	a := map[string]interface{}{
		"a": uint64(rand.Int()),
		"b": map[string]interface{}{
			"c": rand.Float64(),
			"d": []interface{}{uint64(rand.Int()), uint64(rand.Int()), uint64(rand.Int())},
		},
	}

	b := map[string]interface{}{
		"a": uint64(rand.Int()),
		"b": map[string]interface{}{
			"c": rand.Float64(),
			"d": []interface{}{uint64(rand.Int()), uint64(rand.Int()), uint64(rand.Int())},
		},
	}

	aEncoded, err := encodeObject(make([]byte, 0), a)
	require.NoError(t, err)
	bEncoded, err := encodeObject(make([]byte, 0), b)
	require.NoError(t, err)

	require.Equal(t, getSign(encoding.Compare(a, b)),
		getSign(bytes.Compare(aEncoded, bEncoded)))
}

func TestEncodeObject1(t *testing.T) {
	a := map[string]interface{}{
		"a": int64(10),
		"b": "ciao",
	}

	b := map[string]interface{}{
		"a": int64(10),
		"b": int64(11),
	}

	aEncoded, err := encodeObject(make([]byte, 0), a)
	require.NoError(t, err)
	bEncoded, err := encodeObject(make([]byte, 0), b)
	require.NoError(t, err)

	require.Equal(t, getSign(encoding.Compare(a, b)),
		getSign(bytes.Compare(aEncoded, bEncoded)))
}

func TestEncodeObject2(t *testing.T) {
	a := map[string]interface{}{
		"a": int64(10),
		"b": nil,
	}

	b := map[string]interface{}{
		"a": int64(10),
		"b": int64(11),
	}

	aEncoded, err := encodeObject(make([]byte, 0), a)
	require.NoError(t, err)
	bEncoded, err := encodeObject(make([]byte, 0), b)
	require.NoError(t, err)

	require.Equal(t, getSign(encoding.Compare(a, b)),
		getSign(bytes.Compare(aEncoded, bEncoded)))
}

func TestEncodeObject4(t *testing.T) {
	a := map[string]interface{}{
		"a": int64(10),
		"b": true,
	}

	b := map[string]interface{}{
		"a": int64(10),
		"b": int64(0),
	}

	aEncoded, err := encodeObject(make([]byte, 0), a)
	require.NoError(t, err)
	bEncoded, err := encodeObject(make([]byte, 0), b)
	require.NoError(t, err)

	require.Equal(t, getSign(encoding.Compare(a, b)),
		getSign(bytes.Compare(aEncoded, bEncoded)))
}
