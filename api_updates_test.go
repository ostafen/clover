package clover_test

import (
	"testing"

	"github.com/ostafen/clover/v2"
	d "github.com/ostafen/clover/v2/document"
	q "github.com/ostafen/clover/v2/query"
	"github.com/stretchr/testify/require"
)

func TestVariadicUpdateOps(t *testing.T) {
	runCloverTest(t, func(t *testing.T, db *clover.DB) {
		require.NoError(t, db.CreateCollection("users"))

		doc := d.NewDocumentOf(map[string]interface{}{
			"name":  "Alice",
			"age":   30,
			"tags":  []interface{}{"admin"},
			"score": 100,
		})
		_, err := db.Insert("users", doc)
		require.NoError(t, err)

		// Test variadic update ops
		query := q.NewQuery("users").Where(q.Field("name").Eq("Alice"))
		_, count, err := db.Update(query,
			clover.Inc("age", 1),
			clover.Set("role", "superuser"),
			clover.Push("tags", "active"),
			clover.Unset("score"),
		)
		require.NoError(t, err)
		require.Equal(t, 1, count)

		updatedDoc, err := db.FindFirst(query)
		require.NoError(t, err)
		require.NotNil(t, updatedDoc)

		require.Equal(t, float64(31), updatedDoc.Get("age"))
		require.Equal(t, "superuser", updatedDoc.Get("role"))
		require.Equal(t, []interface{}{"admin", "active"}, updatedDoc.Get("tags"))
		require.False(t, updatedDoc.Has("score"))
	})
}

func TestSortOnDisk(t *testing.T) {
	runCloverTest(t, func(t *testing.T, db *clover.DB) {
		require.NoError(t, db.CreateCollection("items"))

		docs := make([]*d.Document, 0, 100)
		for i := 0; i < 100; i++ {
			// Insert in reverse order
			docs = append(docs, d.NewDocumentOf(map[string]interface{}{"val": 100 - i}))
		}
		_, err := db.Insert("items", docs...)
		require.NoError(t, err)

		// Query with sort on disk
		query := q.NewQuery("items").Sort(q.SortOption{Field: "val", Direction: 1}).SortOnDisk()
		sortedDocs, err := db.FindAll(query)
		require.NoError(t, err)
		require.Len(t, sortedDocs, 100)

		for i := 0; i < 100; i++ {
			require.Equal(t, int64(i+1), sortedDocs[i].Get("val"))
		}
	})
}

func TestCompoundIndexIteration(t *testing.T) {
	runCloverTest(t, func(t *testing.T, db *clover.DB) {
		require.NoError(t, db.CreateCollection("points"))
		require.NoError(t, db.CreateIndex("points", "x", "y"))

		// Insert points grid 0-4, 0-4
		docs := make([]*d.Document, 0, 25)
		for i := 0; i < 5; i++ {
			for j := 0; j < 5; j++ {
				docs = append(docs, d.NewDocumentOf(map[string]interface{}{"x": i, "y": j}))
			}
		}
		_, err := db.Insert("points", docs...)
		require.NoError(t, err)

		// Test IterateRange reverse on compound index correctly includes suffix
		query := q.NewQuery("points").Where(q.Field("x").LtEq(2)).Sort(q.SortOption{Field: "x", Direction: -1})
		resultDocs, err := db.FindAll(query)
		require.NoError(t, err)
		
		// Expected x = 2, 1, 0, with 5 items each = 15 items total.
		require.Len(t, resultDocs, 15)
		
		// Verify first 5 are x=2
		for i := 0; i < 5; i++ {
			require.Equal(t, int64(2), resultDocs[i].Get("x"))
		}
	})
}
