package main

import (
	"time"

	c "github.com/ostafen/clover"
)

func main() {
	db, _ := c.Open("clover-db")
	db.CreateCollection("todos")

	// Create todos
	todo1 := c.NewDocument()
	todo1.Set("title", "learn Go1")
	todo1.Set("completed ", false)
	todo1.Set("date", time.Now().Unix())
	todo1.Set("tasks", 3)

	todo2 := c.NewDocument()
	todo2.Set("title", "learn Go2")
	todo2.Set("completed ", false)
	todo2.Set("date", time.Now().Add(time.Hour).Unix())
	todo2.Set("tasks", 2)

	// Insert documents to collection "todos"
	db.InsertOne("todos", todo1)
	db.InsertOne("todos", todo2)

	// Sort todos by id (default)
	db.Query("todos").Sort().FindAll()

	// Sort 'date' field in accesding order (-1 for descending)
	db.Query("todos").Sort(c.SortOption{"date", 1}).FindAll()

	// Sort by number of tasks
	db.Query("todos").Sort(c.SortOption{"tasks", -1}).FindAll()

}
