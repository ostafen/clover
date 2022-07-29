package main

import (
	"fmt"
	"log"

	c "github.com/ostafen/clover/v2"
)

func main() {
	db, _ := c.Open("clover-db")
	defer db.Close()

	// Check if collection already exists
	collectionExists, _ := db.HasCollection("todos")

	if !collectionExists {
		// Create a collection named 'todos'
		db.CreateCollection("todos")
	}

	// Delete collection
	err := db.DropCollection("todos")
	if err != nil {
		log.Panicf("Failed to delete collection: %v", err)
	}
	fmt.Println("collection deleted")
}
