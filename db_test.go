package clover

import (
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func runCloverTest(t *testing.T, dir string, test func(t *testing.T, db *DB)) {
	if dir == "" {
		var err error
		dir, err = ioutil.TempDir("", "clover-test")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, os.RemoveAll(dir))
		}()
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

		doc := NewDocument()
		doc.Set("hello", "clover")

		docId, err := db.InsertOne("myCollection", doc)
		require.NoError(t, err)
		require.NotEmpty(t, docId)

		doc = db.Query("myCollection").FindById(docId)
		require.Equal(t, doc.Get(idFieldName).(string), docId)
	})
}

func TestInsert(t *testing.T) {
	runCloverTest(t, "", func(t *testing.T, db *DB) {
		_, err := db.CreateCollection("myCollection")
		require.NoError(t, err)

		doc := NewDocument()
		doc.Set("hello", "clover")

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
			doc := NewDocument()
			doc.Set("myField", i)
			docs = append(docs, doc)
		}

		require.NoError(t, db.Insert("myCollection", docs...))
		require.Equal(t, nInserts, c.Count())

		n := c.Matches(func(doc *Document) bool {
			require.True(t, doc.Has("myField"))

			v, _ := doc.Get("myField").(float64)
			return int(v)%2 == 0
		}).Count()

		require.Equal(t, nInserts/2, n)
	})
}

func copyCollection(db *DB, src, dst string) error {
	if _, err := db.CreateCollection(dst); err != nil {
		return err
	}
	srcDocs := db.Query(src).FindAll()
	return db.Insert(dst, srcDocs...)
}

func TestUpdateCollection(t *testing.T) {
	runCloverTest(t, "test-data/todos", func(t *testing.T, db *DB) {
		err := copyCollection(db, "todos", "todos-temp")
		require.NoError(t, err)

		defer func() {
			require.NoError(t, db.DropCollection("todos-temp"), err)
		}()

		criteria := Field("completed").Eq(true)
		updates := make(map[string]interface{})
		updates["completed"] = false

		err = db.Query("todos-temp").Where(criteria).Update(updates)
		require.NoError(t, err)

		n := db.Query("todos-temp").Where(criteria).Count()
		require.Equal(t, n, 0)
	})
}

func TestInsertAndDelete(t *testing.T) {
	runCloverTest(t, "test-data/todos", func(t *testing.T, db *DB) {
		err := copyCollection(db, "todos", "todos-temp")
		require.NoError(t, err)

		defer func() {
			require.NoError(t, db.DropCollection("todos-temp"), err)
		}()

		criteria := Field("completed").Eq(true)

		tempTodos := db.Query("todos-temp")
		require.Equal(t, tempTodos.Count(), db.Query("todos").Count())

		err = tempTodos.Where(criteria).Delete()
		require.NoError(t, err)

		// since collection is immutable, we don't see changes in old reference
		tempTodos = db.Query("todos-temp")
		require.Equal(t, tempTodos.Count(), tempTodos.Where(criteria.Not()).Count())
	})
}

func TestOpenExisting(t *testing.T) {
	runCloverTest(t, "test-data/todos", func(t *testing.T, db *DB) {
		require.True(t, db.HasCollection("todos"))
		require.NotNil(t, db.Query("todos"))

		rows := db.Query("todos").Count()
		require.Equal(t, rows, 200)
	})
}

func TestExistsCriteria(t *testing.T) {
	runCloverTest(t, "test-data/todos", func(t *testing.T, db *DB) {
		require.True(t, db.HasCollection("todos"))
		require.NotNil(t, db.Query("todos"))

		docs := db.Query("todos").Where(Field("completed_date").Exists()).FindAll()
		require.Equal(t, len(docs), 1)
	})
}

func TestEqCriteria(t *testing.T) {
	runCloverTest(t, "test-data/todos", func(t *testing.T, db *DB) {
		require.True(t, db.HasCollection("todos"))
		require.NotNil(t, db.Query("todos"))

		docs := db.Query("todos").Where(Field("completed").Eq(true)).FindAll()
		require.Greater(t, len(docs), 0)

		for _, doc := range docs {
			require.NotNil(t, doc.Get("completed"))
			require.Equal(t, doc.Get("completed"), true)
		}
	})
}

func TestEqCriteriaWithDifferentTypes(t *testing.T) {
	runCloverTest(t, "test-data/todos", func(t *testing.T, db *DB) {
		require.True(t, db.HasCollection("todos"))
		require.NotNil(t, db.Query("todos"))

		count1 := db.Query("todos").Where(Field("userId").Eq(int(1))).Count()
		count2 := db.Query("todos").Where(Field("userId").Eq(int8(1))).Count()
		count3 := db.Query("todos").Where(Field("userId").Eq(int16(1))).Count()
		count4 := db.Query("todos").Where(Field("userId").Eq(int32(1))).Count()
		count5 := db.Query("todos").Where(Field("userId").Eq(int64(1))).Count()

		count6 := db.Query("todos").Where(Field("userId").Eq(uint(1))).Count()
		count7 := db.Query("todos").Where(Field("userId").Eq(uint8(1))).Count()
		count8 := db.Query("todos").Where(Field("userId").Eq(uint16(1))).Count()
		count9 := db.Query("todos").Where(Field("userId").Eq(uint32(1))).Count()
		count10 := db.Query("todos").Where(Field("userId").Eq(uint64(1))).Count()

		require.Greater(t, count1, 0)

		require.Equal(t, count1, count2)
		require.Equal(t, count1, count3)
		require.Equal(t, count1, count4)
		require.Equal(t, count1, count5)
		require.Equal(t, count1, count6)
		require.Equal(t, count1, count7)
		require.Equal(t, count1, count8)
		require.Equal(t, count1, count9)
		require.Equal(t, count1, count10)
	})
}

func TestNeqCriteria(t *testing.T) {
	runCloverTest(t, "test-data/todos", func(t *testing.T, db *DB) {
		require.True(t, db.HasCollection("todos"))
		require.NotNil(t, db.Query("todos"))

		docs := db.Query("todos").Where(Field("userId").Neq(7)).FindAll()
		require.Greater(t, len(docs), 0)

		for _, doc := range docs {
			require.NotNil(t, doc.Get("userId"))
			require.NotEqual(t, doc.Get("userId"), float64(7))
		}
	})
}

func TestGtCriteria(t *testing.T) {
	runCloverTest(t, "test-data/todos", func(t *testing.T, db *DB) {
		require.True(t, db.HasCollection("todos"))
		require.NotNil(t, db.Query("todos"))

		docs := db.Query("todos").Where(Field("userId").Gt(4)).FindAll()
		require.Greater(t, len(docs), 0)

		for _, doc := range docs {
			require.NotNil(t, doc.Get("userId"))
			require.Greater(t, doc.Get("userId"), float64(4))
		}
	})
}

func TestGtEqCriteria(t *testing.T) {
	runCloverTest(t, "test-data/todos", func(t *testing.T, db *DB) {
		require.True(t, db.HasCollection("todos"))
		require.NotNil(t, db.Query("todos"))

		docs := db.Query("todos").Where(Field("userId").GtEq(4)).FindAll()
		require.Greater(t, len(docs), 0)

		for _, doc := range docs {
			require.NotNil(t, doc.Get("userId"))
			require.GreaterOrEqual(t, doc.Get("userId"), float64(4))
		}
	})
}

func TestLtCriteria(t *testing.T) {
	runCloverTest(t, "test-data/todos", func(t *testing.T, db *DB) {
		require.True(t, db.HasCollection("todos"))
		require.NotNil(t, db.Query("todos"))

		docs := db.Query("todos").Where(Field("userId").Lt(4)).FindAll()
		require.Greater(t, len(docs), 0)
		for _, doc := range docs {
			require.NotNil(t, doc.Get("userId"))
			require.Less(t, doc.Get("userId"), float64(4))
		}
	})
}

func TestLtEqCriteria(t *testing.T) {
	runCloverTest(t, "test-data/todos", func(t *testing.T, db *DB) {
		require.True(t, db.HasCollection("todos"))
		require.NotNil(t, db.Query("todos"))

		docs := db.Query("todos").Where(Field("userId").LtEq(4)).FindAll()
		require.Greater(t, len(docs), 0)

		for _, doc := range docs {
			require.NotNil(t, doc.Get("userId"))
			require.LessOrEqual(t, doc.Get("userId"), float64(4))
		}
	})
}

func TestInCriteria(t *testing.T) {
	runCloverTest(t, "test-data/todos", func(t *testing.T, db *DB) {
		require.True(t, db.HasCollection("todos"))
		require.NotNil(t, db.Query("todos"))

		docs := db.Query("todos").Where(Field("userId").In(5, 8)).FindAll()

		require.Greater(t, len(docs), 0)

		for _, doc := range docs {
			userId := doc.Get("userId")
			require.NotNil(t, userId)

			if userId != float64(5) && userId != float64(8) {
				require.Fail(t, "userId is not in the correct range")
			}
		}
	})
}

func TestAndCriteria(t *testing.T) {
	runCloverTest(t, "test-data/todos", func(t *testing.T, db *DB) {
		require.True(t, db.HasCollection("todos"))
		require.NotNil(t, db.Query("todos"))

		criteria := Field("completed").Eq(true).And(Field("userId").Gt(2))
		docs := db.Query("todos").Where(criteria).FindAll()

		require.Greater(t, len(docs), 0)
		for _, doc := range docs {
			require.NotNil(t, doc.Get("completed"))
			require.NotNil(t, doc.Get("userId"))
			require.Equal(t, doc.Get("completed"), true)
			require.Greater(t, doc.Get("userId"), float64(2))
		}
	})
}

func TestOrCriteria(t *testing.T) {
	runCloverTest(t, "test-data/airlines", func(t *testing.T, db *DB) {
		require.True(t, db.HasCollection("airlines"))

		criteria := Field("Statistics.Flights.Cancelled").Gt(100).Or(Field("Statistics.Flights.Total").GtEq(1000))
		docs := db.Query("airlines").Where(criteria).FindAll()
		require.Greater(t, len(docs), 0)

		for _, doc := range docs {
			cancelled := doc.Get("Statistics.Flights.Cancelled").(float64)
			total := doc.Get("Statistics.Flights.Total").(float64)

			if cancelled <= 100 && total < 1000 {
				require.Fail(t, "or criteria not satisfied")
			}
		}
	})
}

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
		require.Equal(t, doc.Get(fieldName), i)
	}
}

func TestDocumentUnmarshal(t *testing.T) {
	runCloverTest(t, "test-data/todos", func(t *testing.T, db *DB) {
		require.True(t, db.HasCollection("todos"))
		require.NotNil(t, db.Query("todos"))

		docs := db.Query("todos").FindAll()

		todo := &struct {
			Completed bool   `json:"completed"`
			Title     string `json:"title"`
			UserId    int    `json:"userId"`
		}{}

		require.Greater(t, len(docs), 0)
		for _, doc := range docs {
			err := doc.Unmarshal(todo)
			require.NoError(t, err)
		}
	})
}
