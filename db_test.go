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

func TestInsertAndGet(t *testing.T) {
	runCloverTest(t, "", func(t *testing.T, db *DB) {
		c, err := db.CreateCollection("myCollection")
		require.NoError(t, err)

		nInserts := 100
		docs := make([]*Document, 0, nInserts)
		for i := 0; i < nInserts; i++ {
			doc := newDocument()
			doc.set("myField", i)
			docs = append(docs, doc)
		}

		require.NoError(t, db.Insert("myCollection", docs...))
		require.Equal(t, nInserts, c.Count())

		n := c.Matches(func(doc *Document) bool {
			require.True(t, doc.has("myField"))

			v, _ := doc.get("myField").(float64)
			return int(v)%2 == 0
		}).Count()

		require.Equal(t, nInserts/2, n)
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

		docs := db.Query("todos").Where(Row("completed").Eq(true)).FindAll()
		require.Greater(t, len(docs), 0)

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

		docs := db.Query("todos").Where(Row("userId").Neq(7)).FindAll()
		require.Greater(t, len(docs), 0)

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

		docs := db.Query("todos").Where(Row("userId").Gt(4)).FindAll()
		require.Greater(t, len(docs), 0)

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

		docs := db.Query("todos").Where(Row("userId").GtEq(4)).FindAll()
		require.Greater(t, len(docs), 0)

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

		docs := db.Query("todos").Where(Row("userId").Lt(4)).FindAll()
		require.Greater(t, len(docs), 0)
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

		docs := db.Query("todos").Where(Row("userId").LtEq(4)).FindAll()
		require.Greater(t, len(docs), 0)

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

		docs := db.Query("todos").Where(Row("userId").In(5, 8)).FindAll()

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

		criteria := Row("completed").Eq(true).And(Row("userId").Gt(2))
		docs := db.Query("todos").Where(criteria).FindAll()

		require.Greater(t, len(docs), 0)
		for _, doc := range docs {
			require.NotNil(t, doc.get("completed"))
			require.NotNil(t, doc.get("userId"))
			require.Equal(t, doc.get("completed"), true)
			require.Greater(t, doc.get("userId"), float64(2))
		}
	})
}
