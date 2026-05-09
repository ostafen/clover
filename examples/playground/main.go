package main

import (
	"fmt"
	"os"

	"github.com/ostafen/clover/v2"
	"github.com/ostafen/clover/v2/document"
	"github.com/ostafen/clover/v2/query"
	"github.com/ostafen/clover/v2/store/badger"
)

func main() {
	dbPath := "my-clover-badger"

	// Cleanup previous runs
	os.RemoveAll(dbPath)

	// 1. Open with Badger backend
	st, err := badger.Open(dbPath)
	if err != nil {
		panic(err)
	}
	db, err := clover.OpenWithStore(st)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// 2. Create a collection
	collectionName := "users"
	db.CreateCollection(collectionName)

	// 3. Insert some data
	doc := document.NewDocumentOf(map[string]interface{}{
		"name":    "Alice",
		"age":     25,
		"status":  "active",
		"balance": 100.0,
		"tags":    []string{"golang", "database"},
	})

	err = db.Insert(collectionName, doc)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Inserted document with ID: %s\n", doc.ObjectId())

	// 4. Query the data
	fmt.Println("--- Querying Users ---")
	docs, err := db.FindAll(query.NewQuery(collectionName).Where(query.Field("name").Eq("Alice")))
	if err != nil {
		panic(err)
	}

	for _, d := range docs {
		fmt.Printf("Found: %v\n", d.AsMap())
	}

	// 5. Use the new Variadic Update API
	fmt.Println("\n--- Updating Alice (Atomic) ---")
	err = db.Update(query.NewQuery(collectionName).Where(query.Field("name").Eq("Alice")),
		clover.Inc("age", 1),            // Increment age
		clover.Set("status", "premium"), // Set status
		clover.Push("tags", "atomic"),   // Push to array
		clover.Inc("balance", 50.5),     // Increment balance
	)
	if err != nil {
		panic(err)
	}

	// 6. Verify update
	updatedDoc, _ := db.FindFirst(query.NewQuery(collectionName).Where(query.Field("name").Eq("Alice")))
	if updatedDoc != nil {
		fmt.Printf("Updated Alice: %v\n", updatedDoc.AsMap())
	}
}
