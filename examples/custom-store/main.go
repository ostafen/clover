package main

import (
	"log"

	c "github.com/ostafen/clover/v2"
)

func main() {
	db, err := c.Open("clover-db", map[string]interface{}{"dbStore": "badger"})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
}
