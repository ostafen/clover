package internal

import (
	"bytes"
	"testing"

	"github.com/brianvoe/gofakeit/v6"
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

func TestOrderedCodeObject(t *testing.T) {
	n := 10000
	for i := 0; i < n; i++ {
		a, err := Normalize(gofakeit.Map())
		require.NoError(t, err)
		b, err := Normalize(gofakeit.Map())
		require.NoError(t, err)

		aEncoded, err := OrderedCode(make([]byte, 0), a)
		require.NoError(t, err)
		bEncoded, err := OrderedCode(make([]byte, 0), b)
		require.NoError(t, err)

		require.Equal(t, getSign(Compare(a, b)),
			getSign(bytes.Compare(aEncoded, bEncoded)))
	}
}
