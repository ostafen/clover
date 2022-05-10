<p align="center">
<img alt="CloverDB Logo" src=".github/logo.png#gh-light-mode-only" width="300px">
<img alt="CloverDB Logo" src=".github/logo-white.png#gh-dark-mode-only" width="300px">
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
  GO111MODULE=on go get github.com/ostafen/clover
```

## 数据库和集合
CloverDB将数据记录存储为JSON“文档”，这些“文档“被分组在集合中。数据库由一个或多个集合组成。
以下简称“文档”为文档

### 数据库
要在集合中存储文档，必须使用open()函数打开Clover数据库。 
```go
import (
	"log"
	c "github.com/ostafen/clover"
)

...

db, _ := c.Open("clover-db")

// 或者，如果你不需要持久性，则像下面这样设置开启内存数据库模式
db, _ := c.Open("", c.InMemoryMode(true))

defer db.Close() // 记住当你完成时关闭数据库
```

### 集合
CloverDB将文档存储在集合中。集合是关系数据库中的表的无模式对等物。集合是通过调用数据库实例上的CreateCollection()函数创建的。可以使用Insert()或InsertOne()方法插入新文档。每个文档都由存储在id特殊字段中的Version 4 UUID唯一标识，并在插入期间生成。

```go

db, _ := c.Open("clover-db")
db.CreateCollection("myCollection") // 创建一个名为"mycollection"的新集合

// 在集合中插入一个新文档
doc := c.NewDocument()
doc.Set("hello", "clover!")

// Insertone返回插入文档的ID，此处执行将doc插入到"myCollection"这个collection中
docId, _ := db.InsertOne("myCollection", doc)
fmt.Println(docId)

```
### 引入与导出集合
CloverDB能够轻松地将集合导入和导出为JSON格式，而不管使用的是哪种存储引擎。
```go
// 将"todos"集合的内容转储到"todos.json"文件
db.ExportCollection("todos", "todos.json")

...

// 从导出的json文件中恢复todos集合
db.DropCollection("todos")
db.ImportCollection("todos", "todos.json")

docs, _ := db.Query("todos").FindAll()
for _, doc := range docs {
  log.Println(doc)
}

```


## 请求
CloverDB配备了流利而优雅的API来查询您的数据。查询由查询对象表示，该对象允许检索与给定标准匹配的文档。可以通过将有效的集合名称传递给query()方法来创建查询。


### 选择集合中的所有文档
FindAll()方法用于检索满足给定查询的所有文档。
```go
docs, _ := db.Query("myCollection").FindAll()

todo := &struct {
    Completed bool   `clover:"completed"`
    Title     string `clover:"title"`
    UserId    int    `clover:"userId"`
}{}

for _, doc := range docs {
    doc.Unmarshal(todo)
    log.Println(todo)
}
```
### 筛选器文档与标准
为了过滤FindAll()返回的文档，必须使用Where()方法指定查询标准。标准对象只是表示文档上的谓词，只有当文档满足所有查询条件时才计算为true。

下面的示例展示了如何构建一个简单的标准，以匹配所有completed字段等于true的文档。

```go
db.Query("todos").Where(c.Field("completed").Eq(true)).FindAll()

// 等效于
db.Query("todos").Where(c.Field("completed").IsTrue()).FindAll()
```

为了构建非常复杂的查询，我们使用And()和Or()方法链接多个标准对象，每个对象返回一个通过应用相应的逻辑运算符获得的新标准。
```go
//查找id为5和8的用户的所有已完成的待办事项
db.Query("todos").Where(c.Field("completed").Eq(true).And(c.Field("userId").In(5, 8))).FindAll()
```

### 排序文档
要对CloverDB中的文档进行排序，您需要使用sort()。它是一个可变函数，接受SortOption序列，每个序列允许指定一个字段和一个排序方向。排序方向可以为1或-1，分别对应升序和降序。如果没有提供SortOption, Sort()默认使用id字段。

```go
// 找到属于最近插入的用户的任何待办事项
db.Query("todos").Sort(c.SortOption{"userId", -1}).FindFirst()
```
### 跳过/限制文档
有时，从输出中跳过一些文档，或者简单地设置查询返回结果的最大数量可能很有用。为此，CloverDB提供了Skip()和Limit()函数，它们都接受整数$n$作为参数。
```go
// 丢弃输出中的前10个文档
// 还将查询结果的最大数量限制为100个
db.Query("todos").Skip(10).Limit(100).FindAll()
```


### 更新和删除文档
Update()方法用于修改集合中文档的特定字段。delete()方法用于删除文档。两种方法都属于查询对象，因此易于更新和删除与特定查询匹配的文档。
```go
// 将id为1的用户的所有待办事项标记为已完成
updates := make(map[string]interface{})
updates["completed"] = true

db.Query("todos").Where(c.Field("userId").Eq(1)).Update(updates)

// 删除id为5和8的用户的所有待办事项
db.Query("todos").Where(c.Field("userId").In(5,8)).Delete()
```

要使用特定的文档id更新或删除单个文档，请分别使用UpdateById()或DeleteById(),
顺序为:

```go
docId := "1dbce353-d3c6-43b3-b5a8-80d8d876389b"
// 使用指定的id更新文档
db.Query("todos").UpdateById(docId, map[string]interface{}{"completed": true})
// or delete it
db.Query("todos").DeleteById(docId)
```


## 数据类型
CloverDB内部支持以下原始数据类型：**int64**、**uint64**、**flat64**、**string**、**bool** 和 **time.Time**。CloverDB会尝试对非内部类型进行转化：有符号整数值被转换为int64、而无符号整数值被转换为uint64、Float32值扩展为Float64。


例如，以下代码中的`uint8`类型的值会被CloverDB自动转化：

```go
doc := c.NewDocument()
doc.Set("myField", uint8(10)) // "myField" 被自动转为 uint64 类型

fmt.Println(doc.Get("myField").(uint64))
```

关于指针，将会自动迭代引用，直到迭代出空指针`nil`，或者非指针类型停止：

``` go
var x int = 10
var ptr *int = &x
var ptr1 **int = &ptr

doc.Set("ptr", ptr) // ptr自动迭代指针引用，存入的值为10，下面同理
doc.Set("ptr1", ptr1) 

fmt.Println(doc.Get("ptr").(int64) == 10) // 比较结果为 true
fmt.Println(doc.Get("ptr1").(int64) == 10)

ptr = nil

doc.Set("ptr1", ptr1)
// ptr1为指向ptr的指针，但ptr是一个空指针，所以最终迭代到nil停止，存入值为nil，下方判断为true
fmt.Println(doc.Get("ptr1") == nil)
```

非法数据类型将会被直接丢弃，不触发存入：

```go
doc := c.NewDocument()
doc.Set("myField", make(chan struct{})) // 由于chan非法，所以直接丢弃，不会触发存入

log.Println(doc.Has("myField")) // 这里将会直接打印false
```

## 贡献

**CloverDB** 正在积极开发中。任何以建议、错误报告或拉请求的形式做出的贡献，都是可以接受的。 :blush:

很感激收到的来自下面名单的主要贡献及建议(按字母顺序排列)：

- [ASWLaunchs](https://github.com/ASWLaunchs)
- [jsgm](https://github.com/jsgm)
- [segfault99](https://github.com/segfault99)
