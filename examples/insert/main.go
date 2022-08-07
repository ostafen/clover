package main

import (
	"fmt"

	c "github.com/ostafen/clover/v2"
	d "github.com/ostafen/clover/v2/document"
)

func main() {
	db, _ := c.Open("clover-db")
	defer db.Close()

	// Create a new collection named "todos"
	db.CreateCollection("todos")

	// Create a document
	doc := d.NewDocument()
	doc.Set("title", "ldelectus aut autem")
	doc.Set("completed ", false)

	// InsertOne returns the id of the inserted document
	docId, _ := db.InsertOne("todos", doc)
	fmt.Println(docId)

	// Create document from map
	todo := make(map[string]interface{})
	todo["title"] = "fugiat veniam minus"
	todo["completed"] = false

	// NewDocumentOf creates a document with contents of the provided map
	doc = d.NewDocumentOf(todo)
	title := doc.Get("title")
	fmt.Println(title)

	// Use InsertOne again to insert the document from map
	mapDocId, _ := db.InsertOne("todos", doc)
	fmt.Println(mapDocId)
}
