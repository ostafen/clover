package clover

import (
	"fmt"
	"math/rand"

	"testing"
	"time"

	d "github.com/ostafen/clover/v2/document"
	"github.com/ostafen/clover/v2/query"
)

func setupDB(b *testing.B) *DB {
	dir := fmt.Sprintf("./test-bench-db-%d", time.Now().UnixNano())
	db, err := Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	db.CreateCollection("perf")
	return db
}


func BenchmarkInsert(b *testing.B) {
	db := setupDB(b)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		doc := d.NewDocumentOf(map[string]interface{}{
			"id":    i,
			"value": "test-data-for-benchmark",
		})
		db.Insert("perf", doc)
	}
	b.StopTimer()

	// Clean up using bash command later
	db.Close()
}

func BenchmarkFindById(b *testing.B) {
	db := setupDB(b)
	
	docs := make([]*d.Document, 1000)
	for i := 0; i < 1000; i++ {
		doc := d.NewDocumentOf(map[string]interface{}{
			"test_id": i,
			"value":   "test-data",
		})
		docs[i] = doc
	}
	db.Insert("perf", docs...)
	
	// Create an index for fast lookup
	db.CreateIndex("perf", "test_id")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		target := rand.Intn(1000)
		db.FindFirst(query.NewQuery("perf").Where(query.Field("test_id").Eq(target)))
	}
	b.StopTimer()
	db.Close()
}

func BenchmarkTransactions(b *testing.B) {
	db := setupDB(b)
	
	doc := d.NewDocumentOf(map[string]interface{}{
		"account_id": 1,
		"balance":    10000,
	})
	db.Insert("perf", doc)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, _ := db.Begin(true)
		tx.Update(query.NewQuery("perf").Where(query.Field("account_id").Eq(1)), Inc("balance", -1))
		tx.Commit()
	}
	b.StopTimer()
	db.Close()
}
