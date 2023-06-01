package main

import (
	"fmt"
	"log"
	"time"

	c "github.com/ostafen/clover/v2"
	d "github.com/ostafen/clover/v2/document"
	"github.com/ostafen/clover/v2/query"
)

func main() {
	db, err := c.Open("clover-db")
	if err != nil {
		log.Panicf("Failed to open db: %v", err)
	}
	defer db.Close()

	db.CreateCollection("todos")
	collectionExists, err := db.HasCollection("todos")
	if err != nil {
		log.Panicf("Failed to check collection: %v", err)
	}
	if collectionExists {
		db.Delete(query.NewQuery("todos"))
	}

	// Create todos
	todo1 := d.NewDocument()
	todo1.Set("title", "delectus aut autem")
	todo1.Set("completed ", false)
	todo1.Set("date", time.Now().Unix())
	todo1.Set("tasks", 3)

	todo2 := d.NewDocument()
	todo2.Set("title", "quis ut nam facilis et officia qui")
	todo2.Set("completed ", false)
	todo2.Set("date", time.Now().Add(time.Hour).Unix())
	todo2.Set("tasks", 2)

	// Insert documents to collection "todos"
	db.InsertOne("todos", todo1)
	db.InsertOne("todos", todo2)

	// Sort todos by id (default)
	docs, _ := db.FindAll(query.NewQuery("todos").Sort())

	for _, doc := range docs {
		fmt.Printf("title: %s\n", doc.Get("title"))
	}

	// Sort 'date' field in ascending order
	docs, _ = db.FindAll(query.NewQuery("todos").Sort(query.SortOption{Field: "date", Direction: 1}))

	for _, doc := range docs {
		fmt.Printf("date: %v\n", doc.Get("date"))
	}

	// Sort by number of tasks (-1 for descending)
	docs, _ = db.FindAll(query.NewQuery("todos").Sort(query.SortOption{Field: "tasks", Direction: -1}))

	for _, doc := range docs {
		fmt.Printf("tasks: %v\n", doc.Get("tasks"))
	}
}
