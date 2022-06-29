package clover_test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	c "github.com/ostafen/clover"
)

const (
	airlinesPath = "test/data/airlines.json"
	todosPath    = "test/data/todos.json"
	earthquakes  = "test/data/earthquakes.json"
)

type TodoModel struct {
	Title         string     `json:"title" clover:"title"`
	Completed     bool       `json:"completed,omitempty" clover:"completed"`
	Id            uint       `json:"id" clover:"id"`
	UserId        int        `json:"userId" clover:"userId"`
	CompletedDate *time.Time `json:"completed_date,omitempty" clover:"completed_date,omitempty"`
	Notes         *string    `json:"notes,omitempty" clover:"notes,omitempty"`
}

func runCloverTest(t *testing.T, jsonPath string, dataModel interface{}, test func(t *testing.T, db *c.DB)) {
	dir, err := ioutil.TempDir("", "clover-test")
	require.NoError(t, err)

	inMemDb, err := c.Open("", c.InMemoryMode(true))
	require.NoError(t, err)
	db, err := c.Open(dir)
	require.NoError(t, err)

	if jsonPath != "" {
		require.NoError(t, loadFromJson(inMemDb, jsonPath, dataModel))
		require.NoError(t, loadFromJson(db, jsonPath, dataModel))
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
	runCloverTest(t, "", nil, func(t *testing.T, db *c.DB) {
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
	runCloverTest(t, "", nil, func(t *testing.T, db *c.DB) {
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
	runCloverTest(t, "", nil, func(t *testing.T, db *c.DB) {
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
	runCloverTest(t, "", nil, func(t *testing.T, db *c.DB) {
		err := db.CreateCollection("myCollection")
		require.NoError(t, err)

		doc := c.NewDocument()
		doc.Set("hello", "clover")

		require.NoError(t, db.Insert("myCollection", doc))
		require.Equal(t, db.Insert("myOtherCollection"), c.ErrCollectionNotExist)
	})
}

func TestSaveDocument(t *testing.T) {
	runCloverTest(t, "", nil, func(t *testing.T, db *c.DB) {

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
	runCloverTest(t, "", nil, func(t *testing.T, db *c.DB) {
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

func loadFromJson(db *c.DB, filename string, model interface{}) error {
	var objects []interface{}

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
		data, err := json.Marshal(obj)
		if err != nil {
			return err
		}

		var fields interface{}
		if model == nil {
			fields = make(map[string]interface{})
		} else {
			fields = reflect.New(reflect.TypeOf(model)).Interface()
		}

		if err := json.Unmarshal(data, &fields); err != nil {
			return err
		}

		doc := c.NewDocumentOf(fields)
		docs = append(docs, doc)
	}
	return db.Insert(collectionName, docs...)
}

func TestUpdateCollection(t *testing.T) {
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
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
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
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
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
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
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
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

	require.NoError(t, loadFromJson(db, todosPath, nil))
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
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
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
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
		n, err := db.Query("todos").Where(c.Field("completed_date").Exists()).Count()
		require.NoError(t, err)
		m, err := db.Query("todos").Where(c.Field("completed").IsTrue()).Count()
		require.Equal(t, n, m)
	})
}

func TestNotExistsCriteria(t *testing.T) {
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
		n, err := db.Query("todos").Where(c.Field("completed_date").NotExists()).Count()
		require.NoError(t, err)

		m, err := db.Query("todos").Where(c.Field("completed").IsFalse()).Count()
		require.Equal(t, n, m)
	})
}

func TestIsNil(t *testing.T) {
	runCloverTest(t, todosPath, nil, func(t *testing.T, db *c.DB) {
		n, err := db.Query("todos").Where(c.Field("notes").IsNil()).Count()
		require.NoError(t, err)
		require.Equal(t, n, 1)
	})
}

func TestIsTrue(t *testing.T) {
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
		n, err := db.Query("todos").Where(c.Field("completed").Eq(true)).Count()
		require.NoError(t, err)

		m, err := db.Query("todos").Where(c.Field("completed").IsTrue()).Count()
		require.NoError(t, err)

		require.Equal(t, n, m)
	})
}

func TestIsFalse(t *testing.T) {
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
		n, err := db.Query("todos").Where(c.Field("completed").Eq(false)).Count()
		require.NoError(t, err)

		m, err := db.Query("todos").Where(c.Field("completed").IsFalse()).Count()
		require.NoError(t, err)

		require.Equal(t, n, m)
	})
}

func TestIsNilOrNotExist(t *testing.T) {
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
		n, err := db.Query("todos").Where(c.Field("completed_date").IsNilOrNotExists()).Count()
		require.NoError(t, err)
		m, err := db.Query("todos").Where(c.Field("completed").IsFalse()).Count()
		require.Equal(t, n, m)
	})
}

func TestEqCriteria(t *testing.T) {
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
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
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
		n, err := db.Query("todos").Where(c.Field("completed").Eq(true)).Count()
		require.NoError(t, err)
		m, err := db.Query("todos").Where(c.Field("completed").Gt(false)).Count()
		require.NoError(t, err)

		require.Equal(t, n, m)
	})
}

func TestCompareWithWrongType(t *testing.T) {
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
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
	runCloverTest(t, airlinesPath, nil, func(t *testing.T, db *c.DB) {
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
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
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

func TestCompareUint64(t *testing.T) {
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
		docs, err := db.Query("todos").Where(c.Field("id").Gt(uint64(4))).FindAll()
		require.NoError(t, err)

		for _, doc := range docs {
			require.Greater(t, doc.Get("id"), uint64(4))
		}
	})
}

func TestNeqCriteria(t *testing.T) {
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
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
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
		docs, err := db.Query("todos").Where(c.Field("userId").Gt(4)).FindAll()
		require.NoError(t, err)
		require.Greater(t, len(docs), 0)

		for _, doc := range docs {
			require.NotNil(t, doc.Get("userId"))
			require.Greater(t, doc.Get("userId"), int64(4))
		}
	})
}

func TestGtEqCriteria(t *testing.T) {
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
		docs, err := db.Query("todos").Where(c.Field("userId").GtEq(4)).FindAll()
		require.NoError(t, err)
		require.Greater(t, len(docs), 0)

		for _, doc := range docs {
			require.NotNil(t, doc.Get("userId"))
			require.GreaterOrEqual(t, doc.Get("userId"), int64(4))
		}
	})
}

func TestLtCriteria(t *testing.T) {
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
		docs, err := db.Query("todos").Where(c.Field("userId").Lt(4)).FindAll()
		require.NoError(t, err)

		require.Greater(t, len(docs), 0)
		for _, doc := range docs {
			require.NotNil(t, doc.Get("userId"))
			require.Less(t, doc.Get("userId"), int64(4))
		}
	})
}

func TestLtEqCriteria(t *testing.T) {
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
		docs, err := db.Query("todos").Where(c.Field("userId").LtEq(4)).FindAll()
		require.NoError(t, err)

		require.Greater(t, len(docs), 0)

		for _, doc := range docs {
			require.NotNil(t, doc.Get("userId"))
			require.LessOrEqual(t, doc.Get("userId"), int64(4))
		}
	})
}

func TestInCriteria(t *testing.T) {
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
		docs, err := db.Query("todos").Where(c.Field("userId").In(5, 8)).FindAll()
		require.NoError(t, err)

		require.Greater(t, len(docs), 0)

		for _, doc := range docs {
			userId := doc.Get("userId")
			require.NotNil(t, userId)

			if userId != int64(5) && userId != int64(8) {
				require.Fail(t, "userId is not in the correct range")
			}
		}

		criteria := c.Field("userId").In(c.Field("id"), 6)
		docs, err = db.Query("todos").Where(criteria).FindAll()
		require.NoError(t, err)

		require.Greater(t, len(docs), 0)
		for _, doc := range docs {
			userId := doc.Get("userId").(int64)
			id := doc.Get("id").(uint64)
			require.True(t, uint64(userId) == id || userId == 6)
		}
	})
}

func TestContainsCriteria(t *testing.T) {
	runCloverTest(t, "", nil, func(t *testing.T, db *c.DB) {
		err := db.CreateCollection("myCollection")
		require.NoError(t, err)

		vals := [][]int{
			{
				1, 2, 4,
			},
			{
				5, 6, 7,
			},
			{
				4, 10, 20,
			},
		}
		docs := make([]*c.Document, 0, 3)
		for _, val := range vals {
			doc := c.NewDocument()
			doc.Set("myField", val)
			docs = append(docs, doc)
		}
		require.NoError(t, db.Insert("myCollection", docs...))

		testElement := 4
		docs, err = db.Query("myCollection").Where(c.Field("myField").Contains(testElement)).FindAll()
		require.NoError(t, err)

		require.Equal(t, 2, len(docs))

		for _, doc := range docs {
			myField := doc.Get("myField").([]interface{})
			require.NotNil(t, myField)

			found := false
			for _, elem := range myField {
				if elem.(int64) == 4 {
					found = true
					break
				}
			}

			require.True(t, found, fmt.Sprintf("myField does not contain element %d\n", testElement))

		}
	})
}

func TestChainedWhere(t *testing.T) {
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
		docs, err := db.Query("todos").Where(c.Field("completed").Eq(true)).Where(c.Field("userId").Gt(2)).FindAll()
		require.NoError(t, err)

		require.Greater(t, len(docs), 0)
		for _, doc := range docs {
			require.NotNil(t, doc.Get("completed"))
			require.NotNil(t, doc.Get("userId"))
			require.Equal(t, doc.Get("completed"), true)
			require.Greater(t, doc.Get("userId"), int64(2))
		}
	})
}

func TestAndCriteria(t *testing.T) {
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
		criteria := c.Field("completed").Eq(true).And(c.Field("userId").Gt(2))
		docs, err := db.Query("todos").Where(criteria).FindAll()
		require.NoError(t, err)

		require.Greater(t, len(docs), 0)
		for _, doc := range docs {
			require.NotNil(t, doc.Get("completed"))
			require.NotNil(t, doc.Get("userId"))
			require.Equal(t, doc.Get("completed"), true)
			require.Greater(t, doc.Get("userId"), int64(2))
		}
	})
}

func TestOrCriteria(t *testing.T) {
	runCloverTest(t, airlinesPath, nil, func(t *testing.T, db *c.DB) {
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
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
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

func TestTimeRangeQuery(t *testing.T) {
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
		start := time.Date(2020, 06, 10, 0, 0, 0, 0, time.UTC)
		end := time.Date(2021, 03, 20, 0, 0, 0, 0, time.UTC)

		allDocs, err := db.Query("todos").FindAll()
		require.NoError(t, err)

		n := 0
		for _, doc := range allDocs {
			date := doc.Get("completed_date")
			if date == nil {
				continue
			}
			require.IsType(t, time.Time{}, date)

			completedDate := date.(time.Time)
			if completedDate.Unix() >= start.Unix() && completedDate.Unix() <= end.Unix() {
				n++
			}
		}

		docs, err := db.Query("todos").Where(c.Field("completed_date").GtEq(start).And(c.Field("completed_date").Lt(end))).FindAll()
		require.NoError(t, err)
		require.Len(t, docs, n)

		for _, doc := range docs {
			date := doc.Get("completed_date").(time.Time)
			require.Positive(t, date.Sub(start))
			require.Negative(t, date.Sub(end))
		}
	})
}

func TestLimit(t *testing.T) {
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
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
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
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
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
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
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
		doc, err := db.Query("todos").Where(c.Field("completed").Eq(true)).FindFirst()
		require.NoError(t, err)
		require.NotNil(t, doc)

		require.Equal(t, doc.Get("completed"), true)
	})
}

func TestExists(t *testing.T) {
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
		exists, err := db.Query("todos").Where(c.Field("completed").IsTrue()).Exists()
		require.NoError(t, err)
		require.True(t, exists)

		exists, err = db.Query("todos").Where(c.Field("userId").Eq(100)).Exists()
		require.NoError(t, err)
		require.False(t, exists)
	})
}

func TestForEach(t *testing.T) {
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
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
	runCloverTest(t, airlinesPath, nil, func(t *testing.T, db *c.DB) {
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
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
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
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
		docs, err := db.Query("todos").FindAll()
		require.NoError(t, err)

		todo := &struct {
			Completed     bool       `clover:"completed"`
			Title         string     `clover:"title"`
			UserId        float32    `clover:"userId"`
			CompletedDate *time.Time `clover:"completed_date"`
		}{}

		require.Greater(t, len(docs), 0)
		for _, doc := range docs {
			err := doc.Unmarshal(todo)
			require.NoError(t, err)
		}
	})
}

func TestListCollections(t *testing.T) {
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
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
	runCloverTest(t, todosPath, &TodoModel{}, func(t *testing.T, db *c.DB) {
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
			todo1 := &TodoModel{}
			todo2 := &TodoModel{}

			require.NoError(t, docs[i].Unmarshal(todo1))
			require.NoError(t, importDocs[i].Unmarshal(todo2))

			require.Equal(t, todo1, todo2)
		}
	})
}

func TestSliceCompare(t *testing.T) {
	runCloverTest(t, todosPath, nil, func(t *testing.T, db *c.DB) {
		allDocs, err := db.Query("todos").FindAll()
		require.NoError(t, err)

		require.NoError(t, db.CreateCollection("todos.copy"))

		for _, doc := range allDocs {
			title, _ := doc.Get("title").(string)
			if title != "" {
				s := make([]int, len(title))
				for i := 0; i < len(title); i++ {
					s[i] = int(byte(title[i]))
				}
				doc.Set("title", s)
			}
		}
		err = db.Insert("todos.copy", allDocs...)
		require.NoError(t, err)

		sort1, err := db.Query("todos").Sort(c.SortOption{Field: "title"}).FindAll()

		sort2, err := db.Query("todos.copy").Sort(c.SortOption{Field: "title"}).FindAll()
		require.NoError(t, err)

		require.Equal(t, len(sort1), len(sort2))

		for i := 0; i < len(sort1); i++ {
			doc1 := sort1[i]
			doc2 := sort2[i]

			title := ""
			sTitle := doc2.Get("title").([]interface{})
			for j := 0; j < len(sTitle); j++ {
				title += string(byte(sTitle[j].(int64)))
			}
			require.Equal(t, title, doc1.Get("title"))
		}
	})
}

func TestCompareObjects1(t *testing.T) {
	runCloverTest(t, "", nil, func(t *testing.T, db *c.DB) {
		err := db.CreateCollection("myCollection")
		require.NoError(t, err)

		data := map[string]interface{}{
			"SomeNumber": float64(0),
			"SomeString": "aString",
		}

		dataAsStruct := struct {
			SomeNumber int
			SomeString string
		}{0, "aString"}

		doc := c.NewDocument()
		doc.SetAll(map[string]interface{}{
			"data": data,
		})

		_, err = db.InsertOne("myCollection", doc)
		require.NoError(t, err)

		queryDoc, err := db.Query("myCollection").Where(c.Field("data").Eq(dataAsStruct)).FindFirst()
		require.NoError(t, err)

		require.Equal(t, doc, queryDoc)
	})
}

func TestCompareObjects2(t *testing.T) {
	runCloverTest(t, "", nil, func(t *testing.T, db *c.DB) {
		err := db.CreateCollection("myCollection")
		require.NoError(t, err)

		doc1 := c.NewDocumentOf(map[string]interface{}{
			"data": map[string]interface{}{
				"SomeNumber": float64(0),
				"SomeString": "aString",
			},
		})

		doc2 := c.NewDocumentOf(map[string]interface{}{
			"data": map[string]interface{}{
				"SomeNumber": float64(0),
				"SomeStr":    "aString",
			},
		})

		err = db.Insert("myCollection", doc1, doc2)
		require.NoError(t, err)

		docs, err := db.Query("myCollection").Sort(c.SortOption{Field: "data"}).FindAll()
		require.NoError(t, err)
		require.Len(t, docs, 2)

		require.True(t, docs[0].Has("data.SomeStr"))
	})
}

func TestCompareObjects3(t *testing.T) {
	runCloverTest(t, "", nil, func(t *testing.T, db *c.DB) {
		err := db.CreateCollection("myCollection")
		require.NoError(t, err)

		doc1 := c.NewDocumentOf(map[string]interface{}{
			"data": map[string]interface{}{
				"SomeNumber": float64(0),
				"SomeString": "aString",
			},
		})

		doc2 := c.NewDocumentOf(map[string]interface{}{
			"data": map[string]interface{}{
				"SomeNumber": float64(0),
				"SomeString": "aStr",
			},
		})

		err = db.Insert("myCollection", doc1, doc2)
		require.NoError(t, err)

		docs, err := db.Query("myCollection").Sort(c.SortOption{Field: "data"}).FindAll()
		require.NoError(t, err)
		require.Len(t, docs, 2)

		require.Equal(t, docs[0].Get("data.SomeString"), "aStr")
	})
}

func TestCompareDocumentFields(t *testing.T) {
	runCloverTest(t, airlinesPath, nil, func(t *testing.T, db *c.DB) {
		criteria := c.Field("Statistics.Flights.Diverted").Gt(c.Field("Statistics.Flights.Cancelled"))
		docs, err := db.Query("airlines").Where(criteria).FindAll()
		require.NoError(t, err)

		require.Greater(t, len(docs), 0)
		for _, doc := range docs {
			diverted := doc.Get("Statistics.Flights.Diverted").(float64)
			cancelled := doc.Get("Statistics.Flights.Cancelled").(float64)
			require.Greater(t, diverted, cancelled)
		}

		//alternative syntax using $
		criteria = c.Field("Statistics.Flights.Diverted").Gt("$Statistics.Flights.Cancelled")
		docs, err = db.Query("airlines").Where(criteria).FindAll()
		require.NoError(t, err)

		require.Greater(t, len(docs), 0)
		for _, doc := range docs {
			diverted := doc.Get("Statistics.Flights.Diverted").(float64)
			cancelled := doc.Get("Statistics.Flights.Cancelled").(float64)
			require.Greater(t, diverted, cancelled)
		}
	})
}
