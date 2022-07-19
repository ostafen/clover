package main

import (
	"fmt"
	"time"

	c "github.com/ostafen/clover"
)

func main() {
	db, _ := c.Open("clover-db")
	db.CreateCollection("todos")

	// Create todos
	todo1 := c.NewDocument()
	todo1.Set("title", "delectus aut autem")
	todo1.Set("completed ", false)
	todo1.Set("date", time.Now().Unix())
	todo1.Set("tasks", 3)

	todo2 := c.NewDocument()
	todo2.Set("title", "quis ut nam facilis et officia qui")
	todo2.Set("completed ", false)
	todo2.Set("date", time.Now().Add(time.Hour).Unix())
	todo2.Set("tasks", 2)

	// Insert documents to collection "todos"
	db.InsertOne("todos", todo1)
	db.InsertOne("todos", todo2)

	// Sort todos by id (default)
	docs, _ := db.FindAll(c.NewQuery("todos").Sort())

	for _, doc := range docs {
		fmt.Printf("title: %s\n", doc.Get("title"))
	}

	// Sort 'date' field in ascending order (-1 for descending)
	docs, _ = db.FindAll(c.NewQuery("todos").Sort(c.SortOption{"date", 1}))

	for _, doc := range docs {
		fmt.Printf("date: %v\n", doc.Get("date"))
	}

	// Sort by number of tasks
	docs, _ = db.FindAll(c.NewQuery("airlines").Sort(c.SortOption{"tasks", -1}))

	for _, doc := range docs {
		fmt.Printf("tasks: %v\n", doc.Get("tasks"))
	}
}
