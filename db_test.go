package clover_test

import (
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	c "github.com/ostafen/clover"
	"github.com/stretchr/testify/require"
)

const (
	airlinesPath = "test/data/airlines.json"
	todosPath    = "test/data/todos.json"
	earthquakes  = "test/data/earthquakes.json"
)

func runCloverTest(t *testing.T, jsonPath string, test func(t *testing.T, db *c.DB)) {
	dir, err := ioutil.TempDir("", "clover-test")
	require.NoError(t, err)

	inMemDb, err := c.Open("", c.InMemoryMode(true))
	require.NoError(t, err)
	db, err := c.Open(dir)
	require.NoError(t, err)

	if jsonPath != "" {
		require.NoError(t, loadFromJson(inMemDb, jsonPath))
		require.NoError(t, loadFromJson(db, jsonPath))
	}

	defer func() {
		require.NoError(t, inMemDb.Close())
		require.NoError(t, db.Close())
		require.NoError(t, os.RemoveAll(dir))
	}()
	test(t, db)
	test(t, inMemDb)
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

		_, err = db.InsertOne("myCollection", doc)
		require.Equal(t, c.ErrDuplicateKey, err)

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

func TestSaveDocument(t *testing.T) {
	runCloverTest(t, "", func(t *testing.T, db *c.DB) {

		err := db.CreateCollection("myCollection")
		require.NoError(t, err)

		doc := c.NewDocument()
		doc.Set("hello", "clover")
		require.NoError(t, db.Save("myCollection", doc))

		savedDoc, err := db.Query("myCollection").FindFirst()
		require.NoError(t, err)
		require.Equal(t, savedDoc, doc)

		id := doc.ObjectId()
		doc.Set("_id", id)
		doc.Set("hello", "clover-updated!")
		require.NoError(t, db.Save("myCollection", doc))

		n, err := db.Query("myCollection").Count()
		require.NoError(t, err)
		require.Equal(t, 1, n)

		docUpdated, err := db.Query("myCollection").FindById(id)
		require.NoError(t, err)
		require.Equal(t, doc, docUpdated)
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

			v, _ := doc.Get("myField").(int64)
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
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
		criteria := c.Field("completed").Eq(true)
		updates := make(map[string]interface{})
		updates["completed"] = false

		docs, err := db.Query("todos").Where(criteria).FindAll()
		require.NoError(t, err)

		err = db.Query("todos").Where(criteria).Update(updates)
		require.NoError(t, err)

		n, err := db.Query("todos").Where(criteria).Count()
		require.NoError(t, err)
		require.Equal(t, n, 0)

		for _, doc := range docs {
			doc.Set("completed", false)
			updatedDoc, err := db.Query("todos").Where(c.Field("id").Eq(doc.Get("id"))).FindFirst()
			require.NoError(t, err)
			require.Equal(t, doc, updatedDoc)
		}
	})
}

func TestUpdateById(t *testing.T) {
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
		doc, err := db.Query("todos").FindFirst()
		require.NoError(t, err)

		err = db.Query("todos").UpdateById("invalid-id", map[string]interface{}{})
		require.Error(t, c.ErrDocumentNotExist)

		id := doc.ObjectId()
		completed := doc.Get("completed").(bool)

		err = db.Query("todos").UpdateById(id, map[string]interface{}{"completed": !completed})
		require.NoError(t, err)

		doc, err = db.Query("todos").FindById(id)
		require.NoError(t, err)

		require.Equal(t, !completed, doc.Get("completed").(bool))
	})
}

func TestReplaceById(t *testing.T) {
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
		doc, err := db.Query("todos").FindFirst()
		require.NoError(t, err)

		err = db.Query("todos").ReplaceById("invalid-id", doc)
		require.Error(t, c.ErrDocumentNotExist)

		id := doc.ObjectId()
		newDoc := c.NewDocument()
		newDoc.Set("hello", "clover")

		err = db.Query("todos").ReplaceById(id, newDoc)
		require.Error(t, err)

		newDoc.Set("_id", id)
		err = db.Query("todos").ReplaceById(id, newDoc)
		require.NoError(t, err)

		doc, err = db.Query("todos").FindById(id)
		require.NoError(t, err)
		require.Equal(t, doc, newDoc)
	})
}

func TestInsertAndDelete(t *testing.T) {
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
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

	require.NoError(t, loadFromJson(db, todosPath))
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

func TestReloadIndex(t *testing.T) {
	dir, err := ioutil.TempDir("", "clover-test")
	defer os.RemoveAll(dir)
	require.NoError(t, err)

	db, err := c.Open(dir)
	require.NoError(t, err)

	db.CreateCollection("myCollection")

	doc := c.NewDocument()
	doc.Set("hello", "clover!")

	docId, err := db.InsertOne("myCollection", doc)
	require.NoError(t, err)

	doc, err = db.Query("myCollection").FindById(docId)
	require.NoError(t, err)
	require.Equal(t, docId, doc.ObjectId())

	require.NoError(t, db.Close())

	db, err = c.Open(dir)
	require.NoError(t, err)

	doc, err = db.Query("myCollection").FindById(docId)
	require.NoError(t, err)
	require.Equal(t, docId, doc.ObjectId())

	require.NoError(t, db.Close())
}

func TestInvalidCriteria(t *testing.T) {
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
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
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
		n, err := db.Query("todos").Where(c.Field("completed_date").Exists()).Count()
		require.NoError(t, err)
		m, err := db.Query("todos").Where(c.Field("completed").IsTrue()).Count()
		require.Equal(t, n, m)
	})
}

func TestNotExistsCriteria(t *testing.T) {
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
		n, err := db.Query("todos").Where(c.Field("completed_date").NotExists()).Count()
		require.NoError(t, err)

		m, err := db.Query("todos").Where(c.Field("completed").IsFalse()).Count()
		require.Equal(t, n, m)
	})
}

func TestIsNil(t *testing.T) {
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
		n, err := db.Query("todos").Where(c.Field("notes").IsNil()).Count()
		require.NoError(t, err)
		require.Equal(t, n, 1)
	})
}

func TestIsTrue(t *testing.T) {
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
		n, err := db.Query("todos").Where(c.Field("completed").Eq(true)).Count()
		require.NoError(t, err)

		m, err := db.Query("todos").Where(c.Field("completed").IsTrue()).Count()
		require.NoError(t, err)

		require.Equal(t, n, m)
	})
}

func TestIsFalse(t *testing.T) {
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
		n, err := db.Query("todos").Where(c.Field("completed").Eq(false)).Count()
		require.NoError(t, err)

		m, err := db.Query("todos").Where(c.Field("completed").IsFalse()).Count()
		require.NoError(t, err)

		require.Equal(t, n, m)
	})
}

func TestIsNilOrNotExist(t *testing.T) {
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
		n, err := db.Query("todos").Where(c.Field("completed_date").IsNilOrNotExists()).Count()
		require.NoError(t, err)
		m, err := db.Query("todos").Where(c.Field("completed").IsFalse()).Count()
		require.Equal(t, n, m)
	})
}

func TestEqCriteria(t *testing.T) {
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
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
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
		n, err := db.Query("todos").Where(c.Field("completed").Eq(true)).Count()
		require.NoError(t, err)
		m, err := db.Query("todos").Where(c.Field("completed").Gt(false)).Count()
		require.NoError(t, err)

		require.Equal(t, n, m)
	})
}

func TestCompareWithWrongType(t *testing.T) {
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
		n, err := db.Query("todos").Where(c.Field("completed").Gt("true")).Count()
		require.NoError(t, err)
		require.Equal(t, n, 0)

		n, err = db.Query("todos").Where(c.Field("completed").GtEq("true")).Count()
		require.NoError(t, err)
		require.Equal(t, n, 0)

		n, err = db.Query("todos").Where(c.Field("completed").Lt("true")).Count()
		require.NoError(t, err)
		require.Equal(t, n, 200)

		n, err = db.Query("todos").Where(c.Field("completed").LtEq("true")).Count()
		require.NoError(t, err)
		require.Equal(t, n, 200)
	})
}

func TestCompareString(t *testing.T) {
	runCloverTest(t, airlinesPath, func(t *testing.T, db *c.DB) {
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
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
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

		/*
			count11, err := db.Query("todos").Where(c.Field("userId").Eq(float32(1))).Count()
			require.NoError(t, err)

			count12, err := db.Query("todos").Where(c.Field("userId").Eq(float64(1))).Count()
			require.NoError(t, err)
		*/

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
		//require.Equal(t, count1, count11)
		//require.Equal(t, count1, count12)
	})
}

func TestNeqCriteria(t *testing.T) {
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
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
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
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
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
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
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
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
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
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
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
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
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
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
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
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
	runCloverTest(t, airlinesPath, func(t *testing.T, db *c.DB) {
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

func TestLikeCriteria(t *testing.T) {
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
		likeCriteria := c.Field("title").Like(".*est.*")
		docs, err := db.Query("todos").Where(likeCriteria).FindAll()

		require.NoError(t, err)

		n := len(docs)
		for _, doc := range docs {
			s, isString := doc.Get("title").(string)
			require.True(t, isString)
			require.True(t, strings.Contains(s, "est"))
		}

		docs, err = db.Query("todos").Where(likeCriteria.Not()).FindAll()
		m := len(docs)
		for _, doc := range docs {
			s, isString := doc.Get("title").(string)
			require.True(t, isString)
			require.False(t, strings.Contains(s, "est"))
		}

		total, err := db.Query("todos").Count()
		require.NoError(t, err)
		require.Equal(t, total, n+m)
	})
}

func TestLimit(t *testing.T) {
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
		n, err := db.Query("todos").Count()
		require.NoError(t, err)

		for m := n / 2; m >= 1; m = m / 2 {
			k, err := db.Query("todos").Limit(m).Count()
			require.NoError(t, err)
			require.Equal(t, m, k)
		}
	})
}

func TestSkip(t *testing.T) {
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
		sortOption := c.SortOption{
			Field:     "id",
			Direction: 1,
		}
		allDocs, err := db.Query("todos").Sort(sortOption).FindAll()
		require.NoError(t, err)
		require.Len(t, allDocs, 200)

		skipDocs, err := db.Query("todos").Sort(sortOption).Skip(100).FindAll()
		require.NoError(t, err)

		require.Len(t, skipDocs, 100)
		require.Equal(t, allDocs[100:], skipDocs)
	})
}

func TestLimitAndSkip(t *testing.T) {
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
		sortOption := c.SortOption{
			Field:     "id",
			Direction: 1,
		}
		allDocs, err := db.Query("todos").Sort(sortOption).FindAll()
		require.NoError(t, err)
		require.Len(t, allDocs, 200)

		docs, err := db.Query("todos").Sort(sortOption).Skip(100).Limit(50).FindAll()
		require.NoError(t, err)

		require.Len(t, docs, 50)
		require.Equal(t, allDocs[100:150], docs)
	})
}

func TestFindFirst(t *testing.T) {
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
		doc, err := db.Query("todos").Where(c.Field("completed").Eq(true)).FindFirst()
		require.NoError(t, err)
		require.NotNil(t, doc)

		require.Equal(t, doc.Get("completed"), true)
	})
}

func TestExists(t *testing.T) {
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
		exists, err := db.Query("todos").Where(c.Field("completed").IsTrue()).Exists()
		require.NoError(t, err)
		require.True(t, exists)

		exists, err = db.Query("todos").Where(c.Field("userId").Eq(100)).Exists()
		require.NoError(t, err)
		require.False(t, exists)
	})
}

func TestForEach(t *testing.T) {
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
		n, err := db.Query("todos").Where(c.Field("completed").IsTrue()).Count()
		require.NoError(t, err)

		m := 0
		err = db.Query("todos").Where(c.Field("completed").IsTrue()).ForEach(func(doc *c.Document) bool {
			m++
			return true
		})
		require.NoError(t, err)
		require.Equal(t, n, m)
	})
}

func TestSort(t *testing.T) {
	runCloverTest(t, airlinesPath, func(t *testing.T, db *c.DB) {
		sortOpts := []c.SortOption{{"Statistics.Flights.Total", 1}, {"Statistics.Flights.Cancelled", -1}}

		docs, err := db.Query("airlines").Sort(sortOpts...).FindAll()
		require.NoError(t, err)

		totals := make([]int, 0, len(docs))
		cancelled := make([]int, 0, len(docs))
		for _, doc := range docs {
			if doc.Has("Statistics.Flights.Total") {
				total := int(doc.Get("Statistics.Flights.Total").(float64))
				deleted := int(doc.Get("Statistics.Flights.Cancelled").(float64))

				totals = append(totals, total)
				cancelled = append(cancelled, deleted)
			}
		}

		sorted := sort.SliceIsSorted(docs, func(i, j int) bool {
			if totals[i] != totals[j] {
				return totals[i] < totals[j]
			}
			return cancelled[i] > cancelled[j]
		})
		require.True(t, sorted)
	})
}

func TestForEachStop(t *testing.T) {
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
		n := 0
		err := db.Query("todos").ForEach(func(doc *c.Document) bool {
			if n < 100 {
				n++
				return true
			}
			return false
		})
		require.NoError(t, err)
		require.Equal(t, n, 100)
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

func TestDocumentSetInvalidType(t *testing.T) {
	doc := c.NewDocument()

	// try setting an invalid type
	doc.Set("chan", make(chan struct{}))
	require.Nil(t, doc.Get("chan"))
}

func TestDocumentUnmarshal(t *testing.T) {
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
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

func TestListCollections(t *testing.T) {
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
		collections, err := db.ListCollections()
		require.NoError(t, err)
		require.Equal(t, 1, len(collections))

		err = db.CreateCollection("test1")
		require.NoError(t, err)

		collections, err = db.ListCollections()
		require.Equal(t, 2, len(collections))

		err = db.DropCollection("test1")
		require.NoError(t, err)

		err = db.CreateCollection("c1")
		require.NoError(t, err)
		err = db.CreateCollection("c2")
		require.NoError(t, err)
		err = db.CreateCollection("c3")
		require.NoError(t, err)

		collections, err = db.ListCollections()
		require.Equal(t, 4, len(collections))

		err = db.DropCollection("c1")
		require.NoError(t, err)
		err = db.DropCollection("c2")
		require.NoError(t, err)
		err = db.DropCollection("c3")
		require.NoError(t, err)
		err = db.DropCollection("todos")
		require.NoError(t, err)

		collections, err = db.ListCollections()
		require.Equal(t, 0, len(collections))
	})
}

func TestExportAndImportCollection(t *testing.T) {
	runCloverTest(t, todosPath, func(t *testing.T, db *c.DB) {
		exportPath, err := ioutil.TempDir("", "export-dir")
		require.NoError(t, err)
		defer os.RemoveAll(exportPath)

		exportFilePath := exportPath + "todos.json"
		err = db.ExportCollection("todos", exportFilePath)
		require.NoError(t, err)

		err = db.ImportCollection("todos-copy", exportFilePath)
		require.NoError(t, err)

		docs, err := db.Query("todos").Sort().FindAll()
		require.NoError(t, err)

		importDocs, err := db.Query("todos-copy").Sort().FindAll()
		require.NoError(t, err)

		require.Equal(t, len(docs), len(importDocs))

		for i := 0; i < len(docs); i++ {
			require.Equal(t, docs[i], importDocs[i])
		}
	})
}

func TestSliceCompare(t *testing.T) {
	runCloverTest(t, earthquakes, func(t *testing.T, db *c.DB) {
		coords := []interface{}{127.1311, 6.5061, 26.2}

		n, err := db.Query("earthquakes").Where(c.Field("geometry.coordinates").Eq(coords)).Count()
		require.NoError(t, err)
		require.Equal(t, 1, n)

		n, err = db.Query("earthquakes").Where(c.Field("geometry.coordinates").GtEq(coords)).Count()
		require.NoError(t, err)
		require.Equal(t, 1, n)

		n, err = db.Query("earthquakes").Where(c.Field("geometry.coordinates").Lt(coords)).Count()
		require.NoError(t, err)
		require.Equal(t, 7, n)
	})
}
