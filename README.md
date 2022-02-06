# CloverDB :four_leaf_clover:

[![codecov](https://codecov.io/gh/ostafen/clover/branch/main/graph/badge.svg?token=R06H8FR47O)](https://codecov.io/gh/ostafen/clover)
[![Go Reference](https://pkg.go.dev/badge/badge/github.com/ostafen/clover.svg)](https://pkg.go.dev/github.com/ostafen/clover)

CloverDB is a lightweight NoSQL database designed for being simple and easily maintainable, thanks to its small code base. It has been inspired by [tinyDB](https://github.com/msiemens/tinydb).

# Features

- Document oriented
- Written in pure Golang
- Simple and intuitive api
- Easily maintainable

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

db, _ := c.Open("clover-db")
db.CreateCollection("myCollection")

doc := c.NewDocument()
doc.Set("hello", "clover!")

docId, _ := db.InsertOne("myCollection", doc)

doc = db.Query("myCollection").FindById(docId)
log.Println(doc.Get("hello"))

```

### Query an existing database

```go
db, _ := c.Open("../test-db/")

// find all completed todos belonging to users with id 5 and 8
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

### Update and delete documents

```go
db, _ := c.Open("../test-db/")

// mark all todos belonging to user with id 1 as completed
updates := make(map[string]interface{})
updates["completed"] = true

db.Query("todos").Where(c.Row("userId").Eq(1)).Update(updates)

// delete all todos belonging to users with id 5 and 8
db.Query("todos").Where(c.Row("userId").In(5,8)).Delete()
```

# Contributing

CloverDB is still under development. Any contribution, in the form of a suggestion, bug report or pull request, is well accepted :blush: