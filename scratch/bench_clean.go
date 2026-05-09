package main

import (
	"fmt"
	"time"
	"os"

	"github.com/ostafen/clover/v2"
	"github.com/ostafen/clover/v2/document"
	"github.com/ostafen/clover/v2/query"
	cloverbadger "github.com/ostafen/clover/v2/store/badger"
)

func main() {
	dbPath := "bench-db-clean"
	os.RemoveAll(dbPath)

	st, err := cloverbadger.Open(dbPath)
	if err != nil {
		panic(err)
	}
	db, err := clover.OpenWithStore(st)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	db.CreateCollection("items")

	n := 10000
	fmt.Printf("--- Running Benchmark (n=%d) ---\n", n)

	start := time.Now()
	for i := 0; i < n; i++ {
		doc := document.NewDocumentOf(map[string]interface{}{"id": i, "val": "test"})
		db.Insert("items", doc)
	}
	dur := time.Since(start).Seconds()
	fmt.Printf("Inserts: %.2f ops/sec\n", float64(n)/dur)

	db.CreateIndex("items", "id")
	start = time.Now()
	for i := 0; i < n; i++ {
		db.FindFirst(query.NewQuery("items").Where(query.Field("id").Eq(i)))
	}
	dur = time.Since(start).Seconds()
	fmt.Printf("Reads (Indexed): %.2f ops/sec\n", float64(n)/dur)

	start = time.Now()
	for i := 0; i < n; i++ {
		db.Update(query.NewQuery("items").Where(query.Field("id").Eq(i)), clover.Inc("count", 1))
	}
	dur = time.Since(start).Seconds()
	fmt.Printf("Updates (Atomic): %.2f ops/sec\n", float64(n)/dur)
}
