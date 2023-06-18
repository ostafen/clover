package main

import (
	"log"

	"github.com/dgraph-io/badger/v3"
	c "github.com/ostafen/clover/v2"
	badgerstore "github.com/ostafen/clover/v2/store/badger"
)

func main() {
	store, err := badgerstore.OpenWithOptions(badger.DefaultOptions("").WithInMemory(true))
	if err != nil {
		log.Fatal(err)
	}

	db, err := c.OpenWithStore(store)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
}
