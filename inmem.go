package clover

import (
	"sort"
	"sync"
)

//iterating over a map doesn't guarantee order of the elements
//in order to return same results for every unsorted query
//we also keep insertion id and sort the map elements before querying
type document struct {
	insertId int
	doc      *Document
}

type inMemEngine struct {
	sync.Mutex
	collections map[string]map[string]*document
}

func newInMemoryStoreEngine() StorageEngine {
	return &inMemEngine{
		Mutex:       sync.Mutex{},
		collections: make(map[string]map[string]*document),
	}
}

func InMemoryDB() (*DB, error) {
	return &DB{
		dir:    "",
		engine: newInMemoryStoreEngine(),
	}, nil
}

// Close implements StorageEngine
func (*inMemEngine) Close() error {
	return nil
}

// CreateCollection implements StorageEngine
func (e *inMemEngine) CreateCollection(name string) error {
	e.Lock()
	defer e.Unlock()
	if ok, _ := e.HasCollection(name); ok {
		return ErrCollectionExist
	}
	e.collections[name] = make(map[string]*document)
	return nil
}

// Delete implements StorageEngine
func (e *inMemEngine) Delete(q *Query) error {
	e.Lock()
	defer e.Unlock()

	c, ok := e.collections[q.collection]
	if !ok {
		return ErrCollectionNotExist
	}

	for key, d := range c {
		if q.satisfy(d.doc) {
			delete(c, key)
		}

	}

	return nil
}

// DeleteById implements StorageEngine
func (e *inMemEngine) DeleteById(collectionName string, id string) error {
	e.Lock()
	defer e.Unlock()

	c, ok := e.collections[collectionName]
	if !ok {
		return ErrCollectionNotExist
	}

	delete(c, id)

	return nil
}

func (e *inMemEngine) hasCollection(collectionName string) bool {
	_, ok := e.collections[collectionName]
	return ok
}

// DropCollection implements StorageEngine
func (e *inMemEngine) DropCollection(name string) error {
	e.Lock()
	defer e.Unlock()

	if ok, _ := e.HasCollection(name); ok {
		delete(e.collections, name)
	} else {
		return ErrCollectionNotExist
	}
	return nil
}

// FindAll implements StorageEngine
func (e *inMemEngine) FindAll(q *Query) ([]*Document, error) {
	docs := []*Document{}
	err := e.IterateDocs(q, func(doc *Document) error {
		if q.satisfy(doc) {
			docs = append(docs, doc)
		}
		return nil
	})

	return docs, err
}

// FindById implements StorageEngine
func (e *inMemEngine) FindById(collectionName string, id string) (*Document, error) {
	e.Lock()
	defer e.Unlock()

	c, ok := e.collections[collectionName]
	if !ok {
		return nil, ErrCollectionNotExist
	}

	d := c[id]
	if d == nil {
		return nil, nil
	}
	return d.doc, nil
}

// HasCollection implements StorageEngine
func (e *inMemEngine) HasCollection(name string) (bool, error) {
	_, ok := e.collections[name]
	return ok, nil
}

// Insert implements StorageEngine
func (e *inMemEngine) Insert(collection string, docs ...*Document) error {
	e.Lock()
	defer e.Unlock()

	c, ok := e.collections[collection]
	if !ok {
		return ErrCollectionNotExist
	}

	for _, d := range docs {
		c[d.ObjectId()] = &document{
			insertId: len(c),
			doc:      d,
		}
	}

	return nil
}

// IterateDocs implements StorageEngine
func (e *inMemEngine) IterateDocs(q *Query, consumer docConsumer) error {
	e.Lock()
	defer e.Unlock()

	c, ok := e.collections[q.collection]
	if !ok {
		return ErrCollectionNotExist
	}

	docs := []*document{}

	for _, d := range c {
		docs = append(docs, d)
	}

	sort.Slice(docs, func(i, j int) bool {
		return docs[i].insertId < docs[j].insertId
	})

	skipped := 0
	limited := 0
	sortDocs := len(q.sortOpts) > 0
	if !sortDocs {
		for _, d := range docs {
			if q.skip > 0 && skipped < q.skip {
				skipped++
				continue
			}
			if q.limit >= 0 && q.limit <= limited {
				break
			}
			if q.satisfy(d.doc) {
				limited++
				err := consumer(d.doc)
				if err != nil && err != errStopIteration {
					return err
				}
			}
		}
		return nil
	}

	allDocs := []*Document{}

	for _, d := range c {
		allDocs = append(allDocs, d.doc)

	}

	sort.Slice(allDocs, func(i, j int) bool {
		return compareDocuments(allDocs[i], allDocs[j], q.sortOpts) < 0
	})

	docsToSkip := q.skip
	if len(allDocs) < q.skip {
		docsToSkip = len(allDocs)
	}
	allDocs = allDocs[docsToSkip:]

	if q.limit >= 0 && len(allDocs) > q.limit {
		allDocs = allDocs[:q.limit]
	}

	for _, doc := range allDocs {
		err := consumer(doc)
		if err == errStopIteration {
			return nil
		}

		if err != nil {
			return err
		}
	}

	return nil
}

// Open implements StorageEngine
func (e *inMemEngine) Open(path string) error {
	return nil
}

// Update implements StorageEngine
func (e *inMemEngine) Update(q *Query, updateMap map[string]interface{}) error {
	e.Lock()
	defer e.Unlock()

	c, ok := e.collections[q.collection]
	if !ok {
		return ErrCollectionNotExist
	}

	for _, d := range c {
		if q.satisfy(d.doc) {
			d.doc.fields = updateMap
		}
	}

	return nil
}
