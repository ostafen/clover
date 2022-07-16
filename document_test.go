package clover_test

import (
	"testing"

	c "github.com/ostafen/clover"
	"github.com/stretchr/testify/require"
)

func TestDocument(t *testing.T) {
	doc := c.NewDocument()

	nTests := 1000
	for i := 0; i < nTests; i++ {
		fieldName := genRandomFieldName()
		doc.Set(fieldName, i)
		require.True(t, doc.Has(fieldName))
		require.Equal(t, doc.Get(fieldName), int64(i))
	}
}

func TestDocumentSetUint(t *testing.T) {
	doc := c.NewDocument()

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
	doc := c.NewDocument()

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
	doc := c.NewDocument()

	// test float64 conversion
	doc.Set("float32", float32(0))
	require.IsType(t, float64(0), doc.Get("float32"))

	doc.Set("float64", float64(0))
	require.IsType(t, float64(0), doc.Get("float64"))
}

func TestDocumentSetPointer(t *testing.T) {
	doc := c.NewDocument()

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
	doc := c.NewDocument()

	// try setting an invalid type
	doc.Set("chan", make(chan struct{}))
	require.Nil(t, doc.Get("chan"))
}

func TestDocumentUnmarshal(t *testing.T) {
	runCloverTest(t, func(t *testing.T, db *c.DB) {
		require.NoError(t, loadFromJson(db, todosPath, &TodoModel{}))
		docs, err := db.Query("todos").FindAll()
		require.NoError(t, err)

		todo := &TodoModel{}

		require.Greater(t, len(docs), 0)
		for _, doc := range docs {
			err := doc.Unmarshal(todo)
			require.NoError(t, err)
		}
	})
}

func TestDocumentValidation(t *testing.T) {
	runCloverTest(t, func(t *testing.T, db *c.DB) {
		require.NoError(t, db.CreateCollection("test"))

		doc := c.NewDocument()
		doc.Set("_expiresAt", -1)

		_, err := db.InsertOne("test", doc)
		require.Error(t, err)

		doc = c.NewDocument()
		doc.Set("_id", 0)

		_, err = db.InsertOne("test", doc)
		require.Error(t, err)
	})
}
