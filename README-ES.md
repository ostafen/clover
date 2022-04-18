<p align="center">
<img alt="CloverDB Logo" src=".github/logo.png#gh-light-mode-only" width="300px">
<img alt="CloverDB Logo" src=".github/logo-white.png#gh-dark-mode-only" width="300px">
</p>
<h2 align="center">Base de datos NoSQL ligera orientada a documentos</h2>

[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go)  
[![Go Reference](https://pkg.go.dev/badge/badge/github.com/ostafen/clover.svg)](https://pkg.go.dev/github.com/ostafen/clover)
[![Go Report Card](https://goreportcard.com/badge/github.com/ostafen/clover)](https://goreportcard.com/report/github.com/ostafen/clover)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![codecov](https://codecov.io/gh/ostafen/clover/branch/main/graph/badge.svg?token=R06H8FR47O)](https://codecov.io/gh/ostafen/clover)
[![Join the chat at https://gitter.im/cloverDB/community](https://badges.gitter.im/cloverDB/community.svg)](https://gitter.im/cloverDB/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

> [🇬🇧 English](README.md) | [🇨🇳 简体中文](README-CN.md) | 🇪🇸 Spanish

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

## Bases de datos y colecciones

CloverDB guardar los registros como documentos JSON, los cuales son agrupados en colecciones. Una base de datos por lo tanto está formada de una o más colecciones.

### Base de datos

Para guardar documentos dentro de colecciones, deberás de abrir una base de datos Clover utilizando la función `Open()`.

```go
import (
	"log"
	c "github.com/ostafen/clover"
)

...

db, _ := c.Open("clover-db")
defer db.Close() // recuerda cerrar la base de datos cuando hayas acabado
```

### Colecciones

CloverDB guarda los documentos dentro de colecciones. Las colecciones de bases de datos sin esquema son el equivalente a las tablas en bases de datos relacionales. Una colección se crea llamando a la función `CreateCollection()` en una instancia de la base de datos. Los nuevos documentos pueden ser insertados utilizando los métodos `Insert()` o `InsertOne()`. Cada documento se identifica de forma única por una **Version 4 UUID** guardada en el campo especial **_id** y generado durante la inserción.

```go
db, _ := c.Open("clover-db")
db.CreateCollection("myCollection") // crear una nueva colección llamada "myCollection"

// insertar un nuevo documento dentro de una colección
doc := c.NewDocument()
doc.Set("hello", "clover!")

// InsertOne devuelve el campo id del documento insertado
docId, _ := db.InsertOne("myCollection", doc)
fmt.Println(docId)
```

## Consultas

CloverDB está equipada con una API fluida y elegante para consultar tus datos. Una consulta está representada por el objeto **Query**, que permite devolver documentos que coincidan con unos determinados parámetros. Una consulta puede ser creada pasando como parámetro un nombre de colección válido en el método `Query()`.

### Seleccionar Todos los Documentos en una colección

El método `FindAll()` es utilizado para devolver todos los documentos que coincidan con una determinada consulta.

```go
docs, _ := db.Query("myCollection").FindAll()

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


### Filter Documents with Criteria

Para filtrar los documentos devueltos por `FindAll()`, deberás de especificar ciertos parámetros determinados por el objeto **Criteria** utilizando el método `Where()`. Un objeto **Criteria** simplemente representa una afirmación en un documento, evaluándose como verdadero (true) solo si coinciden todas las condiciones expuestas en la consulta.

El siguiente ejemplo muestra como construir un objeto **Criteria**, que coincida con todos los documentos cuyo campo **completed** sea verdadero (true).

```go
db.Query("todos").Where(c.Field("completed").Eq(true)).FindAll()

// o su equivalente
db.Query("todos").Where(c.Field("completed").IsTrue()).FindAll()
```

Con el objetivo de construir consultas más complejas, encadenaremos diferentes objetos Criteria utilizando los métodos `And()` y `Or()`, each returning a new Criteria obtained by appling the corresponding logical operator.

```go
// encontrar todos los documentos por hacer (todos) que pertenezcan a los usuarios con id 5 y 8
db.Query("todos").Where(c.Field("completed").Eq(true).And(c.Field("userId").In(5, 8))).FindAll()
```

### Ordenar Documentos

Para ordenar documentos en CloverDB, necesitarás usar `Sort()`. Es una función variable que acepta una secuencia de SortOption, cada cual permitirá especificar un campo y una dirección de ordenamiento.
La dirección de ordenamiento puede ser 1 o -1, respectivamente corresponden a orden ascendente y descendente. Si no se proporciona ninguna SortOption, `Sort()` utilizará el campo **_id** por defecto.

```go
// Encontrar cualquier "por hacer" (todo) perteneciente al usuario insertado más reciente
db.Query("todos").Sort(c.SortOption{"userId", -1}).FindFirst()
```

### Saltar/Limitar Documentos

En ocasiones, puede ser útil eliminar ciertos documentos del resultado o simplemente establecer un límite del máximo número de elementos devueltos en una consulta. Para este propósito CloverDB proporciona las funciones `Skip()` y `Limit()`, ambos aceptando un número entero $n$ como parámetro.

```go
// descartar los primeros 10 documentos del resultado,
// y además limitar el número máximo de resultados de la consulta a 100
db.Query("todos").Skip(10).Limit(100).FindAll()
```
### Actualizar/Eliminar Documentos

El método `Update()` es utilizado para modificar campos específicos de documentos en una colección. El método `Delete()` se utiliza para eliminar documentos. Ambos métodos pertenecen al objeto Query, de modo que sea fácil actualizar y eliminar documentos que coincidan con una consulta determinada.

```go
// marcar todos los "por hacer" (todos) que pertenezcan al usuario con id 1 como completado
updates := make(map[string]interface{})
updates["completed"] = true

db.Query("todos").Where(c.Field("userId").Eq(1)).Update(updates)

// eliminar todos los "por hacer" (todos) que pertenezcan a los usuarios con id 5 y 8
db.Query("todos").Where(c.Field("userId").In(5,8)).Delete()
```

Actualizar o eliminar un único documento utilizando la id se puede lograr de la misma forma, using an equality condition on the **_id** field, like shown in the following snippet:

```go
docId := "1dbce353-d3c6-43b3-b5a8-80d8d876389b"
db.Query("todos").Where(c.Field("_id").Eq(docId)).Delete()
```
## Contribuir

**CloverDB** se desarrolla de forma activa. Cualquier contribución, en forma de sugerencia, reporte de errores o pull request es bienvenido :blush:

Se han recibido con gratitud, las principales contribuciones y sugerencias de (en orden alfabético):

- [ASWLaunchs](https://github.com/ASWLaunchs)
- [jsgm](https://github.com/jsgm)
- [segfault99](https://github.com/segfault99)
