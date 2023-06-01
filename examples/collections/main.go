package main

import (
	"fmt"
	"log"

	c "github.com/ostafen/clover/v2"
)

func main() {
	db, err := c.Open("clover-db")
	if err != nil {
		log.Panicf("Failed to open db: %v", err)
	}
	defer db.Close()

	// Check if collection already exists
	collectionExists, err := db.HasCollection("todos")
	if err != nil {
		log.Panicf("Failed to check collection: %v", err)
	}

	if !collectionExists {
		// Create a collection named 'todos'
		db.CreateCollection("todos")
	}

	// Delete collection
	err = db.DropCollection("todos")
	if err != nil {
		log.Panicf("Failed to delete collection: %v", err)
	}
	fmt.Println("collection deleted")
}
