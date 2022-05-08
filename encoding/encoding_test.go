package encoding

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalize(t *testing.T) {
	s := &struct {
		IntField    int
		UintField   uint
		StringField string
	}{}

	ns, err := Normalize(s)
	require.NoError(t, err)

	require.IsType(t, ns, map[string]interface{}{})

	m := ns.(map[string]interface{})

	require.Equal(t, m["IntField"], int64(0))
	require.Equal(t, m["UintField"], uint64(0))
	require.Equal(t, m["StringField"], "")
}
