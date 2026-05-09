package main

import (
	"crypto/rand"
	"fmt"
	"time"
	"os"

	"github.com/dgraph-io/badger/v4"
	"github.com/ostafen/clover/v2"
	"github.com/ostafen/clover/v2/document"
	"github.com/ostafen/clover/v2/query"
	cloverbadger "github.com/ostafen/clover/v2/store/badger"
)

func main() {
	dbPath := "bench-db-large"
	os.RemoveAll(dbPath)

	fmt.Println("--- Creating 10GB BadgerDB (Simulating massive dataset) ---")
	
	opt := badger.DefaultOptions(dbPath).WithLoggingLevel(badger.ERROR)
	bDB, err := badger.Open(opt)
	if err != nil {
		panic(err)
	}

	payload := make([]byte, 1024*1024)
	rand.Read(payload)

	startInflate := time.Now()
	
	wb := bDB.NewWriteBatch()
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("garbage-%d", i))
		err := wb.Set(key, payload)
		if err != nil {
			panic(err)
		}
		
		// Flush every 500MB to avoid the 4GB offset limit in Badger
		if i%500 == 0 && i != 0 {
			wb.Flush()
			wb = bDB.NewWriteBatch()
			fmt.Printf("Inflating... %d/10000 MB\n", i)
		}
	}
	wb.Flush()
	fmt.Printf("Inflation took %.2fs\n", time.Since(startInflate).Seconds())

	bDB.Close()

	fmt.Println("--- Running Benchmark on 10GB Database ---")
	
	badgerStore, err := cloverbadger.OpenWithOptions(opt)
	if err != nil {
		panic(err)
	}
	db, err := clover.OpenWithStore(badgerStore)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	db.CreateCollection("items")

	start := time.Now()
	n := 5000
	for i := 0; i < n; i++ {
		doc := document.NewDocumentOf(map[string]interface{}{"id": i, "val": "test"})
		db.Insert("items", doc)
	}
	dur := time.Since(start).Seconds()
	fmt.Printf("Inserts: %.2f ops/sec (%d in %.2fs)\n", float64(n)/dur, n, dur)

	db.CreateIndex("items", "id")
	start = time.Now()
	for i := 0; i < n; i++ {
		db.FindFirst(query.NewQuery("items").Where(query.Field("id").Eq(i)))
	}
	dur = time.Since(start).Seconds()
	fmt.Printf("Reads (Indexed): %.2f ops/sec (%d in %.2fs)\n", float64(n)/dur, n, dur)

	start = time.Now()
	for i := 0; i < n; i++ {
		tx, _ := db.Begin(true)
		tx.Update(query.NewQuery("items").Where(query.Field("id").Eq(i)), clover.Inc("val", 1))
		tx.Commit()
	}
	dur = time.Since(start).Seconds()
	fmt.Printf("Transactions: %.2f ops/sec (%d in %.2fs)\n", float64(n)/dur, n, dur)
}
