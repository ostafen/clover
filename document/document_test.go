package document

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func genRandomFieldName() string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	size := rand.Intn(100) + 1

	fName := ""
	for i := 0; i < size; i++ {
		fName += "." + string(letters[rand.Intn(len(letters))])
	}
	return fName
}

func TestDocument(t *testing.T) {
	doc := NewDocument()

	nTests := 1000
	for i := 0; i < nTests; i++ {
		fieldName := genRandomFieldName()
		doc.Set(fieldName, i)
		require.True(t, doc.Has(fieldName))
		require.Equal(t, doc.Get(fieldName), int64(i))
	}
}

func TestDocumentSetUint(t *testing.T) {
	doc := NewDocument()

	// test uint64 conversion
	doc.Set("uint", uint(0))
	require.IsType(t, uint64(0), doc.Get("uint"))

	doc.Set("uint8", uint8(0))
	require.IsType(t, uint64(0), doc.Get("uint8"))

	doc.Set("uint16", uint16(0))
	require.IsType(t, uint64(0), doc.Get("uint16"))

	doc.Set("uint32", uint16(0))
	require.IsType(t, uint64(0), doc.Get("uint32"))

	doc.Set("uint64", uint16(0))
	require.IsType(t, uint64(0), doc.Get("uint64"))
}

func TestDocumentSetInt(t *testing.T) {
	doc := NewDocument()

	// test int64 conversion
	doc.Set("int", int(0))
	require.IsType(t, int64(0), doc.Get("int"))

	doc.Set("int8", int8(0))
	require.IsType(t, int64(0), doc.Get("int8"))

	doc.Set("int16", int16(0))
	require.IsType(t, int64(0), doc.Get("int16"))

	doc.Set("int32", int16(0))
	require.IsType(t, int64(0), doc.Get("int32"))

	doc.Set("int64", int16(0))
	require.IsType(t, int64(0), doc.Get("int64"))
}

func TestDocumentSetFloat(t *testing.T) {
	doc := NewDocument()

	// test float64 conversion
	doc.Set("float32", float32(0))
	require.IsType(t, float64(0), doc.Get("float32"))

	doc.Set("float64", float64(0))
	require.IsType(t, float64(0), doc.Get("float64"))
}

func TestDocumentSetPointer(t *testing.T) {
	doc := NewDocument()

	var x int = 100
	ptr := &x
	dPtr := &ptr

	doc.Set("*int", ptr)

	v := doc.Get("*int")

	require.NotEqual(t, &v, ptr)
	require.Equal(t, v, int64(100))

	doc.Set("**int", dPtr)
	v1 := doc.Get("**int")
	require.NotEqual(t, &v1, dPtr)

	require.Equal(t, doc.Get("**int"), int64(100))

	var intPtr *int = nil
	doc.Set("intPtr", intPtr)
	require.True(t, doc.Has("intPtr"))
	require.Nil(t, doc.Get("intPtr"))

	s := "hello"
	var sPtr *string = &s

	doc.Set("string", &sPtr)

	s = "clover" // this statement should not affect the document field

	require.Equal(t, "hello", doc.Get("string"))

	sPtr = nil
	doc.Set("string", sPtr)

	require.True(t, doc.Has("string"))
	require.Nil(t, doc.Get("string"))
}

func TestDocumentSetInvalidType(t *testing.T) {
	doc := NewDocument()

	// try setting an invalid type
	doc.Set("chan", make(chan struct{}))
	require.Nil(t, doc.Get("chan"))
}

func TestDocumentUnmarshal(t *testing.T) {
	a := &struct {
		MyStringField string
	}{"ciao"}

	doc := NewDocumentOf(a)

	b := &struct {
		MyStringField string
	}{}

	require.NoError(t, doc.Unmarshal(b))
	require.Equal(t, a, b)
}

func TestDocumentValidation(t *testing.T) {
	doc := NewDocument()
	doc.Set("_expiresAt", -1)

	doc = NewDocument()
	doc.Set("_id", 0)

	require.Error(t, Validate(doc))
}

func TestDocumentToMap(t *testing.T) {
	doc := NewDocumentOf(map[string]interface{}{
		"f_1": map[string]interface{}{
			"f_1_1": float64(0),
			"f_1_2": "aString",
		},
		"f_2": map[string]interface{}{
			"f_2_1": float64(1),
			"f_2_2": "aString",
		},
		"f_3": int64(42),
	})

	fields := doc.ToMap()
	require.Equal(t, float64(0), fields["f_1"].(map[string]interface{})["f_1_1"])
	require.Equal(t, "aString", fields["f_2"].(map[string]interface{})["f_2_2"])
	require.Equal(t, int64(42), fields["f_3"])
	require.Equal(t, 3, len(fields))
}

func TestDocumentFields(t *testing.T) {
	doc := NewDocumentOf(map[string]interface{}{
		"f_1": map[string]interface{}{
			"f_1_1": float64(0),
			"f_1_2": "aString",
		},
		"f_2": map[string]interface{}{
			"f_2_1": float64(1),
			"f_2_2": "aString",
		},
		"f_3": int64(42),
	})

	keys := doc.Fields(false)
	require.Contains(t, keys, "f_1")
	require.Contains(t, keys, "f_2")
	require.Contains(t, keys, "f_3")
	require.Equal(t, 3, len(keys))

	keys = doc.Fields(true)
	require.Contains(t, keys, "f_1.f_1_1")
	require.Contains(t, keys, "f_1.f_1_2")
	require.Contains(t, keys, "f_2.f_2_1")
	require.Contains(t, keys, "f_2.f_2_2")
	require.Contains(t, keys, "f_3")
	require.Equal(t, 5, len(keys))

}
