<p align="center">
<img width="300" src=".github/logo.png" border="0" alt="kelindar/column">
</p>
<h2 align="center">Base de datos NoSQL ligera orientada a documentos</h2>

[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go)  
[![Go Reference](https://pkg.go.dev/badge/badge/github.com/ostafen/clover.svg)](https://pkg.go.dev/github.com/ostafen/clover)
[![Go Report Card](https://goreportcard.com/badge/github.com/ostafen/clover)](https://goreportcard.com/report/github.com/ostafen/clover)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![codecov](https://codecov.io/gh/ostafen/clover/branch/main/graph/badge.svg?token=R06H8FR47O)](https://codecov.io/gh/ostafen/clover)
[![Join the chat at https://gitter.im/cloverDB/community](https://badges.gitter.im/cloverDB/community.svg)](https://gitter.im/cloverDB/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

> [English](README.md) | [简体中文](README-CN.md) | Spanish

**CloverDB** es una base de datos NoSQL diseñada para ser simple y de fácil mantenimiento, gracias a no tener demasiado código. Está inspirada en [tinyDB](https://github.com/msiemens/tinydb).

## Características

- Orientada a documentos
- Programada en puro Golang
- API simple e intuitiva
- Mantenimiento sencillo

## ¿Por qué CloverDB?

**CloverDB** ha sido programada para tener un mantenimiento sencillo. Por lo tanto, intercambia rendimiento con simplicidad y no está intencionada para ser una alternativa a bases de datos más eficientes como **MongoDB** o **MySQL**. Sin embargo, existen proyectos donde ejecutar una base de datos en un servidor por separado puede ser excesivo, y, para consultas simples, los retrasos de la red pueden ser el principal cuello de botella en el rendimiento. Para ese escenario, **CloverDB** puede ser una alternativa más adecuada.

## Diseño de la base de datos

**CloverDB** abstrae la forma en que las colecciones se almacenan en el disco mediante la interfaz **StorageEngine**. La implementación por defecto está basada en almacenamiento de tipo clave-valor de la base de datos [Badger](https://github.com/dgraph-io/badger). Además, puedes escribir fácilmente tu propia implementación para el motor de almacenamiento.

## Instalación
Asegúrate de que tienes un entorno Go funcional (Se requiere Go 1.13 o superior). 
```shell
  go get github.com/ostafen/clover
```

## Uso de la API

```go
import (
	"log"
	c "github.com/ostafen/clover"
)

...

```

### Crear una nueva colección
```go

db, _ := c.Open("clover-db")
db.CreateCollection("myCollection")

doc := c.NewDocument()
doc.Set("hello", "clover!")

docId, _ := db.InsertOne("myCollection", doc)

doc, _ = db.Query("myCollection").FindById(docId)
log.Println(doc.Get("hello"))

```

### Consultar a una base de datos existente

```go
db, _ := c.Open("../test-data/todos")

// buscar todos los "por hacer" (todos) pertenecientes al usuario con id 5 y 8
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

### Actualizar y eliminar documentos

```go
db, _ := c.Open("../test-data/todos")

// marcar todos los "por hacer" (todos) pertenecientes al usuario con id 1 como completados
updates := make(map[string]interface{})
updates["completed"] = true

db.Query("todos").Where(c.Field("userId").Eq(1)).Update(updates)

// buscar todos los "por hacer" (todos) pertenecientes al usuario con id 5 y 8
db.Query("todos").Where(c.Field("userId").In(5,8)).Delete()
```

## Contribuir

**CloverDB** se desarrolla de forma activa. Cualquier contribución, en forma de sugerencia, reporte de errores o pull request es bienvenido :blush: