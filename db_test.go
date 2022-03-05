package clover_test

import (
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	c "github.com/ostafen/clover"
	"github.com/stretchr/testify/require"
)

func runCloverTest(t *testing.T, jsonPath string, test func(t *testing.T, db *c.DB)) {
	dir, err := ioutil.TempDir("", "clover-test")
	require.NoError(t, err)

	db, err := c.Open(dir)
	require.NoError(t, err)

	if jsonPath != "" {
		require.NoError(t, loadFromJson(db, jsonPath))
	}
	defer func() {
		require.NoError(t, db.Close())
		require.NoError(t, os.RemoveAll(dir))
	}()
	test(t, db)
}

func TestErrCollectionNotExist(t *testing.T) {
	runCloverTest(t, "", func(t *testing.T, db *c.DB) {
		q := db.Query("myCollection")
		_, err := q.Count()
		require.Equal(t, c.ErrCollectionNotExist, err)

		_, err = q.FindById("objectId")
		require.Equal(t, c.ErrCollectionNotExist, err)

		_, err = q.FindAll()
		require.Equal(t, c.ErrCollectionNotExist, err)

		err = q.Update(nil)
		require.Equal(t, c.ErrCollectionNotExist, err)

		err = q.Delete()
		require.Equal(t, c.ErrCollectionNotExist, err)

		err = q.DeleteById("objectId")
		require.Equal(t, c.ErrCollectionNotExist, err)

		err = db.DropCollection("myCollection")
		require.Equal(t, c.ErrCollectionNotExist, err)
	})
}

func TestCreateCollectionAndDrop(t *testing.T) {
	runCloverTest(t, "", func(t *testing.T, db *c.DB) {
		err := db.CreateCollection("myCollection")
		require.NoError(t, err)
		has, err := db.HasCollection("myCollection")
		require.NoError(t, err)
		require.True(t, has)

		err = db.CreateCollection("myCollection")
		require.Equal(t, err, c.ErrCollectionExist)

		err = db.DropCollection("myCollection")
		require.NoError(t, err)

		has, err = db.HasCollection("myCollection")
		require.NoError(t, err)
		require.False(t, has)

		err = db.DropCollection("myOtherCollection")
		require.Equal(t, err, c.ErrCollectionNotExist)
	})
}

func TestInsertOneAndDelete(t *testing.T) {
	runCloverTest(t, "", func(t *testing.T, db *c.DB) {
		err := db.CreateCollection("myCollection")
		require.NoError(t, err)

		doc := c.NewDocument()
		doc.Set("hello", "clover")

		require.Empty(t, doc.ObjectId())
		docId, err := db.InsertOne("myCollection", doc)
		require.NoError(t, err)
		require.NotEmpty(t, docId)

		doc, err = db.Query("myCollection").FindById(docId)
		require.NoError(t, err)
		require.NotEmpty(t, doc.ObjectId(), docId)

		err = db.Query("myCollection").DeleteById(docId)
		require.NoError(t, err)

		doc, err = db.Query("myCollection").FindById(docId)
		require.NoError(t, err)
		require.Nil(t, doc)

		n, err := db.Query("myCollection").Count()
		require.NoError(t, err)
		require.Equal(t, n, 0)
	})
}

func TestInsert(t *testing.T) {
	runCloverTest(t, "", func(t *testing.T, db *c.DB) {
		err := db.CreateCollection("myCollection")
		require.NoError(t, err)

		doc := c.NewDocument()
		doc.Set("hello", "clover")

		require.NoError(t, db.Insert("myCollection", doc))
		require.Equal(t, db.Insert("myOtherCollection"), c.ErrCollectionNotExist)
	})
}

func TestInsertAndGet(t *testing.T) {
	runCloverTest(t, "", func(t *testing.T, db *c.DB) {
		err := db.CreateCollection("myCollection")
		require.NoError(t, err)

		nInserts := 100
		docs := make([]*c.Document, 0, nInserts)
		for i := 0; i < nInserts; i++ {
			doc := c.NewDocument()
			doc.Set("myField", i)
			docs = append(docs, doc)
		}

		require.NoError(t, db.Insert("myCollection", docs...))
		n, err := db.Query("myCollection").Count()
		require.NoError(t, err)
		require.Equal(t, nInserts, n)

		n, err = db.Query("myCollection").MatchPredicate(func(doc *c.Document) bool {
			require.True(t, doc.Has("myField"))

			v, _ := doc.Get("myField").(float64)
			return int(v)%2 == 0
		}).Count()
		require.NoError(t, err)

		require.Equal(t, nInserts/2, n)
	})
}

func loadFromJson(db *c.DB, filename string) error {
	objects := make([]map[string]interface{}, 0)

	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(data, &objects); err != nil {
		return err
	}

	collectionName := strings.TrimSuffix(filepath.Base(filename), ".json")
	if err := db.CreateCollection(collectionName); err != nil {
		return err
	}

	docs := make([]*c.Document, 0)
	for _, obj := range objects {
		doc := c.NewDocumentOf(obj)
		docs = append(docs, doc)
	}

	return db.Insert(collectionName, docs...)
}

func TestUpdateCollection(t *testing.T) {
	runCloverTest(t, "test-data/todos.json", func(t *testing.T, db *c.DB) {
		criteria := c.Field("completed").Eq(true)
		updates := make(map[string]interface{})
		updates["completed"] = false

		err := db.Query("todos").Where(criteria).Update(updates)
		require.NoError(t, err)

		n, err := db.Query("todos").Where(criteria).Count()
		require.NoError(t, err)
		require.Equal(t, n, 0)
	})
}

func TestInsertAndDelete(t *testing.T) {
	runCloverTest(t, "test-data/todos.json", func(t *testing.T, db *c.DB) {
		criteria := c.Field("completed").Eq(true)
		err := db.Query("todos").Where(criteria).Delete()
		require.NoError(t, err)

		n, err := db.Query("todos").Count()
		require.NoError(t, err)

		m, err := db.Query("todos").Where(criteria.Not()).Count()
		require.NoError(t, err)

		require.Equal(t, n, m)
	})
}

func TestOpenExisting(t *testing.T) {
	dir, err := ioutil.TempDir("", "clover-test")
	defer os.RemoveAll(dir)
	require.NoError(t, err)

	db, err := c.Open(dir)
	require.NoError(t, err)

	require.NoError(t, loadFromJson(db, "test-data/todos.json"))
	require.NoError(t, db.Close())

	db, err = c.Open(dir)
	require.NoError(t, err)

	has, err := db.HasCollection("todos")
	require.NoError(t, err)
	require.True(t, has)

	rows, err := db.Query("todos").Count()
	require.NoError(t, err)
	require.Equal(t, 200, rows)
}

func TestInvalidCriteria(t *testing.T) {
	runCloverTest(t, "test-data/todos.json", func(t *testing.T, db *c.DB) {
		docs, err := db.Query("todos").Where(c.Field("completed").Eq(func() {})).FindAll()
		require.NoError(t, err)
		require.Equal(t, len(docs), 0)

		docs, err = db.Query("todos").Where(c.Field("completed").Neq(func() {})).FindAll()
		require.NoError(t, err)

		n, err := db.Query("todos").Count()
		require.NoError(t, err)
		require.Equal(t, len(docs), n)

		docs, err = db.Query("todos").Where(c.Field("completed").Lt(func() {})).FindAll()
		require.NoError(t, err)

		require.Equal(t, len(docs), 0)

		docs, err = db.Query("todos").Where(c.Field("completed").LtEq(func() {})).FindAll()
		require.NoError(t, err)
		require.Equal(t, len(docs), 0)

		docs, err = db.Query("todos").Where(c.Field("completed").Gt(func() {})).FindAll()
		require.NoError(t, err)
		require.Equal(t, len(docs), 0)

		docs, err = db.Query("todos").Where(c.Field("completed").GtEq(func() {})).FindAll()
		require.NoError(t, err)
		require.Equal(t, len(docs), 0)
	})
}

func TestExistsCriteria(t *testing.T) {
	runCloverTest(t, "test-data/todos.json", func(t *testing.T, db *c.DB) {
		docs, err := db.Query("todos").Where(c.Field("completed_date").Exists()).FindAll()
		require.NoError(t, err)
		require.Equal(t, len(docs), 1)
	})
}

func TestEqCriteria(t *testing.T) {
	runCloverTest(t, "test-data/todos.json", func(t *testing.T, db *c.DB) {
		docs, err := db.Query("todos").Where(c.Field("completed").Eq(true)).FindAll()
		require.NoError(t, err)
		require.Greater(t, len(docs), 0)

		for _, doc := range docs {
			require.NotNil(t, doc.Get("completed"))
			require.Equal(t, doc.Get("completed"), true)
		}
	})
}

func TestBoolCompare(t *testing.T) {
	runCloverTest(t, "test-data/todos.json", func(t *testing.T, db *c.DB) {
		n, err := db.Query("todos").Where(c.Field("completed").Eq(true)).Count()
		require.NoError(t, err)
		m, err := db.Query("todos").Where(c.Field("completed").Gt(false)).Count()
		require.NoError(t, err)

		require.Equal(t, n, m)
	})
}

func TestCompareWithWrongType(t *testing.T) {
	runCloverTest(t, "test-data/todos.json", func(t *testing.T, db *c.DB) {
		n, err := db.Query("todos").Where(c.Field("completed").Gt("true")).Count()
		require.NoError(t, err)
		require.Equal(t, n, 0)

		n, err = db.Query("todos").Where(c.Field("completed").GtEq("true")).Count()
		require.NoError(t, err)
		require.Equal(t, n, 0)

		n, err = db.Query("todos").Where(c.Field("completed").Lt("true")).Count()
		require.NoError(t, err)
		require.Equal(t, n, 0)

		n, err = db.Query("todos").Where(c.Field("completed").LtEq("true")).Count()
		require.NoError(t, err)
		require.Equal(t, n, 0)
	})
}

func TestCompareString(t *testing.T) {
	runCloverTest(t, "test-data/airlines.json", func(t *testing.T, db *c.DB) {
		docs, err := db.Query("airlines").Where(c.Field("Airport.Code").Gt("CLT")).FindAll()
		require.NoError(t, err)
		require.Greater(t, len(docs), 0)

		for _, doc := range docs {
			code := doc.Get("Airport.Code").(string)
			require.NotNil(t, code)
			require.Greater(t, code, "CLT")
		}
	})
}

func TestEqCriteriaWithDifferentTypes(t *testing.T) {
	runCloverTest(t, "test-data/todos.json", func(t *testing.T, db *c.DB) {
		count1, err := db.Query("todos").Where(c.Field("userId").Eq(int(1))).Count()
		require.NoError(t, err)

		count2, err := db.Query("todos").Where(c.Field("userId").Eq(int8(1))).Count()
		require.NoError(t, err)

		count3, err := db.Query("todos").Where(c.Field("userId").Eq(int16(1))).Count()
		require.NoError(t, err)

		count4, err := db.Query("todos").Where(c.Field("userId").Eq(int32(1))).Count()
		require.NoError(t, err)

		count5, err := db.Query("todos").Where(c.Field("userId").Eq(int64(1))).Count()
		require.NoError(t, err)

		count6, err := db.Query("todos").Where(c.Field("userId").Eq(uint(1))).Count()
		require.NoError(t, err)

		count7, err := db.Query("todos").Where(c.Field("userId").Eq(uint8(1))).Count()
		require.NoError(t, err)

		count8, err := db.Query("todos").Where(c.Field("userId").Eq(uint16(1))).Count()
		require.NoError(t, err)

		count9, err := db.Query("todos").Where(c.Field("userId").Eq(uint32(1))).Count()
		require.NoError(t, err)

		count10, err := db.Query("todos").Where(c.Field("userId").Eq(uint64(1))).Count()
		require.NoError(t, err)

		count11, err := db.Query("todos").Where(c.Field("userId").Eq(float32(1))).Count()
		require.NoError(t, err)

		count12, err := db.Query("todos").Where(c.Field("userId").Eq(float64(1))).Count()
		require.NoError(t, err)

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
		require.Equal(t, count1, count11)
		require.Equal(t, count1, count12)
	})
}

func TestNeqCriteria(t *testing.T) {
	runCloverTest(t, "test-data/todos.json", func(t *testing.T, db *c.DB) {
		docs, err := db.Query("todos").Where(c.Field("userId").Neq(7)).FindAll()
		require.NoError(t, err)
		require.Greater(t, len(docs), 0)

		for _, doc := range docs {
			require.NotNil(t, doc.Get("userId"))
			require.NotEqual(t, doc.Get("userId"), float64(7))
		}
	})
}

func TestGtCriteria(t *testing.T) {
	runCloverTest(t, "test-data/todos.json", func(t *testing.T, db *c.DB) {
		docs, err := db.Query("todos").Where(c.Field("userId").Gt(4)).FindAll()
		require.NoError(t, err)
		require.Greater(t, len(docs), 0)

		for _, doc := range docs {
			require.NotNil(t, doc.Get("userId"))
			require.Greater(t, doc.Get("userId"), float64(4))
		}
	})
}

func TestGtEqCriteria(t *testing.T) {
	runCloverTest(t, "test-data/todos.json", func(t *testing.T, db *c.DB) {
		docs, err := db.Query("todos").Where(c.Field("userId").GtEq(4)).FindAll()
		require.NoError(t, err)
		require.Greater(t, len(docs), 0)

		for _, doc := range docs {
			require.NotNil(t, doc.Get("userId"))
			require.GreaterOrEqual(t, doc.Get("userId"), float64(4))
		}
	})
}

func TestLtCriteria(t *testing.T) {
	runCloverTest(t, "test-data/todos.json", func(t *testing.T, db *c.DB) {
		docs, err := db.Query("todos").Where(c.Field("userId").Lt(4)).FindAll()
		require.NoError(t, err)

		require.Greater(t, len(docs), 0)
		for _, doc := range docs {
			require.NotNil(t, doc.Get("userId"))
			require.Less(t, doc.Get("userId"), float64(4))
		}
	})
}

func TestLtEqCriteria(t *testing.T) {
	runCloverTest(t, "test-data/todos.json", func(t *testing.T, db *c.DB) {
		docs, err := db.Query("todos").Where(c.Field("userId").LtEq(4)).FindAll()
		require.NoError(t, err)

		require.Greater(t, len(docs), 0)

		for _, doc := range docs {
			require.NotNil(t, doc.Get("userId"))
			require.LessOrEqual(t, doc.Get("userId"), float64(4))
		}
	})
}

func TestInCriteria(t *testing.T) {
	runCloverTest(t, "test-data/todos.json", func(t *testing.T, db *c.DB) {
		docs, err := db.Query("todos").Where(c.Field("userId").In(5, 8)).FindAll()
		require.NoError(t, err)

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

func TestChainedWhere(t *testing.T) {
	runCloverTest(t, "test-data/todos.json", func(t *testing.T, db *c.DB) {
		docs, err := db.Query("todos").Where(c.Field("completed").Eq(true)).Where(c.Field("userId").Gt(2)).FindAll()
		require.NoError(t, err)

		require.Greater(t, len(docs), 0)
		for _, doc := range docs {
			require.NotNil(t, doc.Get("completed"))
			require.NotNil(t, doc.Get("userId"))
			require.Equal(t, doc.Get("completed"), true)
			require.Greater(t, doc.Get("userId"), float64(2))
		}
	})
}

func TestAndCriteria(t *testing.T) {
	runCloverTest(t, "test-data/todos.json", func(t *testing.T, db *c.DB) {
		criteria := c.Field("completed").Eq(true).And(c.Field("userId").Gt(2))
		docs, err := db.Query("todos").Where(criteria).FindAll()
		require.NoError(t, err)

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
	runCloverTest(t, "test-data/airlines.json", func(t *testing.T, db *c.DB) {
		criteria := c.Field("Statistics.Flights.Cancelled").Gt(100).Or(c.Field("Statistics.Flights.Total").GtEq(1000))
		docs, err := db.Query("airlines").Where(criteria).FindAll()
		require.NoError(t, err)

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
	doc := c.NewDocument()

	nTests := 1000
	for i := 0; i < nTests; i++ {
		fieldName := genRandomFieldName()
		doc.Set(fieldName, i)
		require.True(t, doc.Has(fieldName))
		require.Equal(t, doc.Get(fieldName), i)
	}
}

func TestDocumentUnmarshal(t *testing.T) {
	runCloverTest(t, "test-data/todos.json", func(t *testing.T, db *c.DB) {
		docs, err := db.Query("todos").FindAll()
		require.NoError(t, err)

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

func Test_Marshal(t *testing.T) {
	type Array struct {
		Strings []string
	}
	type Test struct {
		Name  string
		Value int
		List  Array
	}
	expected := Test{
		Name:  "abc",
		Value: 23,
		List: Array{
			Strings: []string{"X", "Y", "Z"},
		},
	}
	var got Test

	d, err := c.Marshal(expected)
	if err != nil {
		t.Errorf("did not expect an error marshaling here")
	}
	d.Unmarshal(&got)

	if expected.Name != got.Name {
		t.Errorf("expected %q, got %q", expected.Name, got.Name)
	}
	if expected.Value != got.Value {
		t.Errorf("expected %q, got %q", expected.Value, got.Value)
	}
	for i, v := range expected.List.Strings {
		g := got.List.Strings[i]
		if v != g {
			t.Errorf("expected %q, got %q", v, g)
		}
	}
}
