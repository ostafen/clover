<p align="center">
<img width="300" src=".github/logo.png" border="0" alt="kelindar/column">
</p>
<h2 align="center">轻量级面向文档的NoSQL数据库</h2>

[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go)  
[![Go Reference](https://pkg.go.dev/badge/badge/github.com/ostafen/clover.svg)](https://pkg.go.dev/github.com/ostafen/clover)
[![Go Report Card](https://goreportcard.com/badge/github.com/ostafen/clover)](https://goreportcard.com/report/github.com/ostafen/clover)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![codecov](https://codecov.io/gh/ostafen/clover/branch/main/graph/badge.svg?token=R06H8FR47O)](https://codecov.io/gh/ostafen/clover)
[![Join the chat at https://gitter.im/cloverDB/community](https://badges.gitter.im/cloverDB/community.svg)](https://gitter.im/cloverDB/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

> [🇬🇧 English](README.md) | 🇨🇳 简体中文 | [🇪🇸 Spanish](README-ES.md) 

**CloverDB** 是一个轻量级的NoSQL数据库，由于它的代码库很小，所以设计得简单且易于维护。它的灵感来自 [tinyDB](https://github.com/msiemens/tinydb).

## 特点

- 面向文档
- 原生Golang编写
- 简单直观的api
- 容易维护

## 为什么选择CloverDB?

编写**CloverDB**是为了使其易于维护。因此，它以简单性换取性能，并不是为了替代性能更好的数据库，如**MongoDB**或**MySQL**。然而，在有些项目中，运行单独的数据库服务器可能会导致过度消耗，并且，对于简单的查询，网络延迟可能是主要的性能瓶颈。对于这个场景，**cloverDB**可能是一个更合适的替代方案。

## 数据层

**CloverDB**通过**StorageEngine**抽象的方式将集合存储在磁盘上。默认的实现基于[Badger](https://github.com/dgraph-io/badger)数据库键值存储。不管怎样
，您可以轻松地编写自己的存储引擎实现。

## 安装
确保你拥有Go运行环境 (需要Go 1.13 或者更高版本)
```shell
  go get github.com/ostafen/clover
```

## API 用法

```go
import (
	"log"
	c "github.com/ostafen/clover"
)

...

```

### 创造一个新的集合
```go

db, _ := c.Open("clover-db")
db.CreateCollection("myCollection")

doc := c.NewDocument()
doc.Set("hello", "clover!")

docId, _ := db.InsertOne("myCollection", doc)

doc, _ = db.Query("myCollection").FindById(docId)
log.Println(doc.Get("hello"))

```

### 请求现有的数据库

```go
db, _ := c.Open("../test-data/todos")

//找到属于id为5和8的用户的所有completed等于true的todos。
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

### 更新和删除文档

```go
db, _ := c.Open("../test-data/todos")

// 将所有属于id为1的用户的todos的completed更新为true。
updates := make(map[string]interface{})
updates["completed"] = true

db.Query("todos").Where(c.Field("userId").Eq(1)).Update(updates)

// 将所有id为5和8的用户的todos删除。
db.Query("todos").Where(c.Field("userId").In(5,8)).Delete()
```

### 更新一个单独的文档
```go
db, _ := c.Open("../test-data/todos")

updates := make(map[string]interface{})
updates["completed"] = true

// 您可以得到 _id
doc, _ := db.Query("todos").Where(c.Field("userId").Eq(2)).FindFirst()
docId := doc.Get("_id")

// 或者使用一段字符串
// docId := "1dbce353-d3c6-43b3-b5a8-80d8d876389b"

//根据_id更新单独的文档
db.Query("todos").Where(c.Field("_id").Eq(docId)).Update(updates)
```

## 贡献

**CloverDB** 正在积极开发中。任何以建议、错误报告或拉请求的形式做出的贡献，都是可以接受的。 :blush:
