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

**CloverDB** ha sido programada para tener un mantenimiento sencillo. Por lo tanto, intercambia rendimiento con simplicidad y no está intencionada para ser una alternativa a bases de datos más eficientes como **MongoDB** o **MySQL**. Sin embargo, existen proyectos donde ejecutar una base de datos en un servidor por separado puede ser excesivo, y, para consultas simples, los retrasos de la red pueden ser el principal cuello de botella en el rendimiento. Para este tipo de escenarios, **CloverDB** puede ser una alternativa más adecuada.

## Diseño de la base de datos

**CloverDB** abstrae la forma en que las colecciones se almacenan en el disco mediante la interfaz **StorageEngine**. La implementación por defecto está basada en almacenamiento de tipo clave-valor de la base de datos [Badger](https://github.com/dgraph-io/badger).

## Instalación
Asegúrate de que tienes un entorno Go funcional (Se requiere Go 1.13 o superior). 
```shell
  GO111MODULE=on go get github.com/ostafen/clover
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

// o bien, si no necesitas persistencia en los datos
db, _ := c.Open("", c.InMemoryMode(true))

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

### Importar y Exportar Colecciones

CloverDB es capaz de importar y exportar colecciones sencillamente a formato JSON independientemente del motor de almacenamiento usado.

```go
// vuelca el contenido de la colección "todos" en un fichero JSON "todos.json"
db.ExportCollection("todos", "todos.json")

...

// recupera la colección "todos" del fichero JSON exportado
db.DropCollection("todos")
db.ImportCollection("todos", "todos.json")

docs, _ := db.FindAll(c.NewQuery("todos"))
for _, doc := range docs {
  log.Println(doc)
}
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


### Filtrar Documentos

Para filtrar los documentos devueltos por `FindAll()`, deberás de especificar ciertos parámetros determinados por el objeto **Criteria** utilizando el método `Where()`. Un objeto **Criteria** simplemente representa una afirmación en un documento, evaluándose como verdadero (true) solo si coinciden todas las condiciones expuestas en la consulta.

El siguiente ejemplo muestra como construir un objeto **Criteria**, que coincida con todos los documentos cuyo campo **completed** sea verdadero (true).

```go
db.FindAll(c.NewQuery("todos").Where(c.Field("completed").Eq(true)))

// o bien, su equivalente
db.FindAll(c.NewQuery("todos").Where(c.Field("completed").IsTrue()))
```

Con el objetivo de construir consultas más complejas, encadenaremos diferentes objetos Criteria utilizando los métodos `And()` y `Or()`, cada uno de ellos devolverá un nuevo objeto Criteria obtenido mediante la aplicación de los correspondientes operadores lógicos.

```go
// encontrar todos los documentos por hacer (todos) que pertenezcan a los usuarios con id 5 y 8
db.FindAll(c.NewQuery("todos").Where(c.Field("completed").Eq(true).And(c.Field("userId").In(5, 8))))
```

Naturalmente,  también podrás crear un objeto **Criteria** que implique múltiples campos. CloverDB te proporciona dos formas equivalentes de lograr esto:

```go
db.FindAll(c.NewQuery("myCollection").Where(c.Field("myField1").Gt(c.Field("myField2"))))

// o bien, si lo prefieres
db.FindAll(c.NewQuery("myCollection").Where(c.Field("myField1").Gt("$myField2")))
```

### Ordenar Documentos

Para ordenar documentos en CloverDB, necesitarás usar `Sort()`. Es una función variable que acepta una secuencia de SortOption, cada cual permitirá especificar un campo y una dirección de ordenamiento.
La dirección de ordenamiento puede ser 1 o -1, respectivamente corresponden a orden ascendente y descendente. Si no se proporciona ninguna SortOption, `Sort()` utilizará el campo **_id** por defecto.

```go
// Encontrar cualquier "por hacer" (todo) perteneciente al usuario insertado más reciente
db.FindFirst(c.NewQuery("todos").Sort(c.SortOption{"userId", -1}))
```

### Saltar/Limitar Documentos

En ocasiones, puede ser útil eliminar ciertos documentos del resultado o simplemente establecer un límite del máximo número de elementos devueltos en una consulta. Para este propósito CloverDB proporciona las funciones `Skip()` y `Limit()`, ambos aceptando un número entero $n$ como parámetro.

```go
// descartar los primeros 10 documentos del resultado,
// y además limitar el número máximo de resultados de la consulta a 100
db.FindAll(c.NewQuery("todos").Skip(10).Limit(100))
```
### Actualizar/Eliminar Documentos

El método `Update()` es utilizado para modificar campos específicos de documentos en una colección. El método `Delete()` se utiliza para eliminar documentos. Ambos métodos pertenecen al objeto Query, de modo que sea fácil actualizar y eliminar documentos que coincidan con una consulta determinada.

```go
// marcar todos los "por hacer" (todos) que pertenezcan al usuario con id 1 como completado
updates := make(map[string]interface{})
updates["completed"] = true

db.Update(c.NewQuery("todos").Where(c.Field("userId").Eq(1)), updates)

// eliminar todos los "por hacer" (todos) que pertenezcan a los usuarios con id 5 y 8
db.Delete(c.NewQuery("todos").Where(c.Field("userId").In(5,8)))
```

Actualizar o eliminar un único documento utilizando la id se puede lograr de la misma forma, usando `UpdateById()` o `DeleteById()`, respectivamente:

```go
docId := "1dbce353-d3c6-43b3-b5a8-80d8d876389b"
// actualiza el documento con la id especificada
db.UpdateById("todos", docId, map[string]interface{}{"completed": true})
// o elimínalo
db.DeleteById("todos", docId)
```

## Índices
En CloverDB, los índices apoyan la ejecución eficiente de consultas. Sin índices, una colección debe ser escaneada por completo para seleccionar aquellos documentos que coincidan con una determinada consulta. Un índice es una estructura especial de datos que guarda los valores (o grupo de valores) de un campo específico de un documento, ordenado por el valor del propio campo. Esto significa que los índices pueden ser utilizados para ayudar a realizar consultas eficientes de coincidencias de igualdad y consultas basadas en rangos. Además, cuando los documentos son iterados a través de un índice, los resultados se devolverán ya ordenados sin necesidad de ejecutar ningún paso de ordenación adicional.
Sin embargo deberás de tener en cuenta que el uso de índices no sale gratis por completo. Además de incrementar el uso del disco, los índices requieren tiempo de CPU adicional durante las operaciones de inserción y actualización/borrado. Además, cuando se accede a un documento a través de un índice, se ejecutarán dos lecturas de disco ya que el índice únicamente guarda la referencia de la id del documento real. Como consecuencia, el aumento de velocidad solo se notará cuando los criterios especificados son usados para acceder a un grupo restringido de documentos.

### Crear un índice

Actualmente, CloverDB únicamente soporta índices de un único campo. Un índice puede ser creado simplemente llamando al método `CreateIndex()`, que obtendrá tanto el nombre de la colección como el campo a indexar.

```go
db.CreateIndex("myCollection", "myField")
```

Suponiendo que tienes la siguiente consulta:

```go
criteria := c.Field("myField").Gt(a).And(c.Field("myField").Lt(b))
db.FindAll(c.NewQuery("myCollection").Where(criteria).Sort(c.SortOption{"myField", -1}))
```

donde **a** y **b** son valores de tu elección. CloverDB utilizará el índice creado para realizar tanto la consulta del rango como para devolver los resultados ordenados.

## Tipos de datos

Internamente, CloverDB soporta los siguientes tipos de datos primitivos: **int64**, **uint64**, **float64**, **string**, **bool** y **time.Time**. Cuando es posible, valores con diferentes tipos son convertidos de forma automática a alguno de los siguientes tipos de datos: valores enteros se convertirán a int64, mientras que aquellos que no tienen signo lo harán a uint64. Los valores de float32 se extenderán a float64.

Por ejemplo, considera el siguiente fragmento de código, que establece un valor de uint8 en el campo del documento:

```go
doc := c.NewDocument()
doc.Set("myField", uint8(10)) // "myField" se intercambia automáticamente a uint64

fmt.Println(doc.Get("myField").(uint64))
```

En los valores de los punteros se eliminarán las referencias hasta que se encuentre un **nil** o un valor **non-pointer**:

``` go
var x int = 10
var ptr *int = &x
var ptr1 **int = &ptr

doc.Set("ptr", ptr)
doc.Set("ptr1", ptr1)

fmt.Println(doc.Get("ptr").(int64) == 10)
fmt.Println(doc.Get("ptr1").(int64) == 10)

ptr = nil

doc.Set("ptr1", ptr1)
// ptr1 no es nil, pero apunta al puntero nil "ptr", por lo tanto el campo se establecerá en nil
fmt.Println(doc.Get("ptr1") == nil)
```

Los tipos de datos inválidos dejarán el documento tal cual está:

```go
doc := c.NewDocument()
doc.Set("myField", make(chan struct{}))

log.Println(doc.Has("myField")) // devolverá 'false'
```

## Contribuir

**CloverDB** se desarrolla de forma activa. Cualquier contribución, en forma de sugerencia, reporte de errores o pull request es bienvenido :blush:

Se han recibido con gratitud, las principales contribuciones y sugerencias de (en orden alfabético):

- [ASWLaunchs](https://github.com/ASWLaunchs)
- [jsgm](https://github.com/jsgm)
- [segfault99](https://github.com/segfault99)
