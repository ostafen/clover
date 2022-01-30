# CloverDB :four_leaf_clover:

[![codecov](https://codecov.io/gh/ostafen/cloverDB/branch/main/graph/badge.svg?token=R06H8FR47O)](https://codecov.io/gh/ostafen/cloverDB)

CloverDB is a lightweight NoSQL database designed for being simple and easily maintenable, due to its small code base. It has been inspired by [tinyDB](https://github.com/msiemens/tinydb).

# Features

- Document oriented
- Written in pure Golang
- Simple and intuitive api
- Easily maintenable

# API usage

```go
import (
	"log"
	c "github.com/ostafen/clover"
)

...

```

### Create a new collection

```go

db, _ := clover.Open("clover-db")
db.CreateCollection("myCollection")

doc := clover.NewDocument()
doc.Set("hello", "clover!")

docId, _ := db.InsertOne("myCollection", doc)

doc = db.Query("myCollection").FindById(docId)
log.Println(doc.Get("hello"))

```

### Query an existing database

```go

db, _ := c.Open("../test-db/")

// find all completed todos belongin to users with id 5 and 8
c := db.Query("todos").Where(c.Row("completed").Eq(true).And(c.Row("userId").In(5, 8)))

todo := &struct {
    Completed bool   `json:"completed"`
    Title     string `json:"title"`
    UserId    int    `json:"userId"`
}{}

for _, doc := range c.FindAll() {
    doc.Unmarshal(todo)
    log.Println(todo)
}
```
