package main

import (
	"fmt"

	c "github.com/ostafen/clover/v2"
	d "github.com/ostafen/clover/v2/document"
	"github.com/ostafen/clover/v2/query"
)

func main() {
	db, _ := c.Open("clover-db")
	defer db.Close()

	db.CreateCollection("todos")

	// Create todos
	todo1 := d.NewDocument()
	todo2 := d.NewDocument()

	todo1.Set("title", "delectus aut autem")
	todo1.Set("completed", false)
	todo1.Set("userId", 1)

	todo2.Set("title", "quis ut nam facilis et officia qui")
	todo2.Set("completed", false)
	todo2.Set("userId", 2)

	db.Insert("todos", todo1, todo2)

	// export collection 'todos' to 'dump.json' file
	db.ExportCollection("todos", "dump.json")

	// delete collection 'todos'
	db.DropCollection("todos")

	// restore collection from json file
	db.ImportCollection("todos", "dump.json")

	todos, _ := db.FindAll(query.NewQuery("todos"))

	for _, todo := range todos {
		fmt.Printf("title: %s\n", todo.Get("title"))
	}
}
