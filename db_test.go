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

func TestEqCriteria(t *testing.T) {
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

func TestNeqCriteria(t *testing.T) {
	runCloverTest(t, "test-db", func(t *testing.T, db *DB) {
		require.True(t, db.HasCollection("todos"))
		require.NotNil(t, db.Query("todos"))

		docs := db.Query("todos").Where(row("userId").Neq(7)).FindAll()
		for _, doc := range docs {
			require.NotNil(t, doc.get("userId"))
			require.NotEqual(t, doc.get("userId"), float64(7))
		}
	})
}

func TestGtCriteria(t *testing.T) {
	runCloverTest(t, "test-db", func(t *testing.T, db *DB) {
		require.True(t, db.HasCollection("todos"))
		require.NotNil(t, db.Query("todos"))

		docs := db.Query("todos").Where(row("userId").Gt(4)).FindAll()
		for _, doc := range docs {
			require.NotNil(t, doc.get("userId"))
			require.Greater(t, doc.get("userId"), float64(4))
		}
	})
}

func TestGtEqCriteria(t *testing.T) {
	runCloverTest(t, "test-db", func(t *testing.T, db *DB) {
		require.True(t, db.HasCollection("todos"))
		require.NotNil(t, db.Query("todos"))

		docs := db.Query("todos").Where(row("userId").GtEq(4)).FindAll()
		for _, doc := range docs {
			require.NotNil(t, doc.get("userId"))
			require.GreaterOrEqual(t, doc.get("userId"), float64(4))
		}
	})
}

func TestLtCriteria(t *testing.T) {
	runCloverTest(t, "test-db", func(t *testing.T, db *DB) {
		require.True(t, db.HasCollection("todos"))
		require.NotNil(t, db.Query("todos"))

		docs := db.Query("todos").Where(row("userId").Lt(4)).FindAll()
		for _, doc := range docs {
			require.NotNil(t, doc.get("userId"))
			require.Less(t, doc.get("userId"), float64(4))
		}
	})
}

func TestLtEqCriteria(t *testing.T) {
	runCloverTest(t, "test-db", func(t *testing.T, db *DB) {
		require.True(t, db.HasCollection("todos"))
		require.NotNil(t, db.Query("todos"))

		docs := db.Query("todos").Where(row("userId").LtEq(4)).FindAll()
		for _, doc := range docs {
			require.NotNil(t, doc.get("userId"))
			require.LessOrEqual(t, doc.get("userId"), float64(4))
		}
	})
}
func TestInCriteria(t *testing.T) {
	runCloverTest(t, "test-db", func(t *testing.T, db *DB) {
		require.True(t, db.HasCollection("todos"))
		require.NotNil(t, db.Query("todos"))

		docs := db.Query("todos").Where(row("userId").In(5, 8)).FindAll()

		require.Greater(t, len(docs), 0)

		for _, doc := range docs {
			userId := doc.get("userId")
			require.NotNil(t, userId)

			if userId != float64(5) && userId != float64(8) {
				require.Fail(t, "userId is not in the correct range")
			}
		}
	})
}

func TestAndCriteria(t *testing.T) {
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
