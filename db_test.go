package clover

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func runCloverTest(t *testing.T, dir string, test func(t *testing.T, db *DB)) {
	if dir == "" {
		var err error
		dir, err = ioutil.TempDir("", "clover-test")
		require.NoError(t, err)
		defer os.RemoveAll(dir)
	}
	db, err := Open(dir)
	require.NoError(t, err)

	test(t, db)
}

func TestCreateCollection(t *testing.T) {
	runCloverTest(t, "", func(t *testing.T, db *DB) {
		_, err := db.CreateCollection("myCollection")
		require.NoError(t, err)
		require.True(t, db.HasCollection("myCollection"))
	})
}

func TestInsertOne(t *testing.T) {
	runCloverTest(t, "", func(t *testing.T, db *DB) {
		_, err := db.CreateCollection("myCollection")
		require.NoError(t, err)

		doc := newDocument()
		doc.set("hello", "clover")

		docId, err := db.InsertOne("myCollection", doc)
		require.NoError(t, err)
		require.NotEmpty(t, docId)
	})
}

func TestInsert(t *testing.T) {
	runCloverTest(t, "", func(t *testing.T, db *DB) {
		_, err := db.CreateCollection("myCollection")
		require.NoError(t, err)

		doc := newDocument()
		doc.set("hello", "clover")

		require.NoError(t, db.Insert("myCollection", doc))
	})
}

func TestOpenExisting(t *testing.T) {
	runCloverTest(t, "test-db", func(t *testing.T, db *DB) {
		require.True(t, db.HasCollection("todos"))
		require.NotNil(t, db.Query("todos"))

		rows := db.Query("todos").Count()
		require.Equal(t, rows, 200)
	})
}

func TestSimpleWhere(t *testing.T) {
	runCloverTest(t, "test-db", func(t *testing.T, db *DB) {
		require.True(t, db.HasCollection("todos"))
		require.NotNil(t, db.Query("todos"))

		docs := db.Query("todos").Where(row("completed").Eq(true)).FindAll()
		for _, doc := range docs {
			require.NotNil(t, doc.get("completed"))
			require.Equal(t, doc.get("completed"), true)
		}
	})
}

func TestWhereWithAnd(t *testing.T) {
	runCloverTest(t, "test-db", func(t *testing.T, db *DB) {
		require.True(t, db.HasCollection("todos"))
		require.NotNil(t, db.Query("todos"))

		criteria := row("completed").Eq(true).And(row("userId").Gt(2))
		docs := db.Query("todos").Where(criteria).FindAll()

		for _, doc := range docs {
			require.NotNil(t, doc.get("completed"))
			require.NotNil(t, doc.get("userId"))
			require.Equal(t, doc.get("completed"), true)
			require.Greater(t, doc.get("userId"), float64(2))
		}
	})
}
