<p align="center">
<img width="300" src=".github/logo.png" border="0" alt="kelindar/column">
</p>
<h2 align="center">Lightweight document-oriented NoSQL Database</h2>

[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go)  
[![Go Reference](https://pkg.go.dev/badge/badge/github.com/ostafen/clover.svg)](https://pkg.go.dev/github.com/ostafen/clover)
[![Go Report Card](https://goreportcard.com/badge/github.com/ostafen/clover)](https://goreportcard.com/report/github.com/ostafen/clover)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![codecov](https://codecov.io/gh/ostafen/clover/branch/main/graph/badge.svg?token=R06H8FR47O)](https://codecov.io/gh/ostafen/clover)
[![Join the chat at https://gitter.im/cloverDB/community](https://badges.gitter.im/cloverDB/community.svg)](https://gitter.im/cloverDB/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

> English | [简体中文](README-CN.md)

**CloverDB** is a lightweight NoSQL database designed for being simple and easily maintainable, thanks to its small code base. It has been inspired by [tinyDB](https://github.com/msiemens/tinydb).

## Features

- Document oriented
- Written in pure Golang
- Simple and intuitive api
- Easily maintainable

## Why CloverDB?

**CloverDB** has been written for being easily maintenable. As such, it trades performance with simplicity, and is not intented to be an alternative to more performant databases such as **MongoDB** or **MySQL**.
However, there are projects where running a separate database server may result overkilled, and, for simple queries, network delay may be the major performance bottleneck.
For there scenario, **CloverDB** may be a more suitable alternative.

## Database layout

**CloverDB** abstracts the way collections are stored on disk through the **StorageEngine** interface. The default implementation is based on the [Badger](https://github.com/dgraph-io/badger) database key-value store. However, you could easily write your own storage engine implementation.

## Installation
Make sure you have a working Go environment (Go 1.13 or higher is required). 
```shell
  go get github.com/ostafen/clover
```

## API usage

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

doc, _ = db.Query("myCollection").FindById(docId)
log.Println(doc.Get("hello"))

```

### Query an existing database

```go
db, _ := c.Open("../test-data/todos")

// find all completed todos belonging to users with id 5 and 8
docs, _ := db.Query("todos").Where(c.Field("completed").Eq(true).And(c.Field("userId").In(5, 8))).FindAll()

todo := &struct {
    Completed bool   `json:"completed"`
    Title     string `json:"title"`
    UserId    int    `json:"userId"`
}{}

for _, doc := range docs {
    doc.Unmarshal(todo)
    log.Println(todo)
}
```

### Update and delete documents

```go
db, _ := c.Open("../test-data/todos")

// mark all todos belonging to user with id 1 as completed
updates := make(map[string]interface{})
updates["completed"] = true

db.Query("todos").Where(c.Field("userId").Eq(1)).Update(updates)

// delete all todos belonging to users with id 5 and 8
db.Query("todos").Where(c.Field("userId").In(5,8)).Delete()
```

## Contributing

**CloverDB** is actively developed. Any contribution, in the form of a suggestion, bug report or pull request, is well accepted :blush: