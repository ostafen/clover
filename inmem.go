package clover

import (
	"sort"
	"sync"
)

type collection map[string]*Document

type inMemEngine struct {
	sync.RWMutex
	collections map[string]collection
}

func newInMemoryStoreEngine() StorageEngine {
	return &inMemEngine{
		RWMutex:     sync.RWMutex{},
		collections: make(map[string]collection),
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
	e.collections[name] = make(collection)
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
		if q.satisfy(d) {
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
	e.RLock()
	defer e.RUnlock()

	c, ok := e.collections[collectionName]
	if !ok {
		return nil, ErrCollectionNotExist
	}

	return c[id], nil
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
		c[d.ObjectId()] = d
	}

	return nil
}

func (e *inMemEngine) iterateDocsSlice(q *Query, consumer docConsumer) error {
	c, ok := e.collections[q.collection]
	if !ok {
		return ErrCollectionNotExist
	}

	sortDocs := len(q.sortOpts) > 0
	allDocs := []*Document{}

	for _, d := range c {
		if q.satisfy(d) {
			allDocs = append(allDocs, d)
		}
	}

	if sortDocs {
		sort.Slice(allDocs, func(i, j int) bool {
			return compareDocuments(allDocs[i], allDocs[j], q.sortOpts) < 0
		})
	}

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

// IterateDocs implements StorageEngine
func (e *inMemEngine) IterateDocs(q *Query, consumer docConsumer) error {
	e.RLock()
	defer e.RUnlock()

	return e.iterateDocsSlice(q, consumer)
}

// Open implements StorageEngine
func (e *inMemEngine) Open(path string) error {
	return nil
}

// Update implements StorageEngine
func (e *inMemEngine) Update(q *Query, updateMap map[string]interface{}) error {
	e.Lock()
	defer e.Unlock()
	return e.replaceDocs(q, func(doc *Document) *Document {
		updateDoc := doc.Copy()
		for updateField, updateValue := range updateMap {
			updateDoc.Set(updateField, updateValue)
		}
		return updateDoc
	})
}

func (s *inMemEngine) replaceDocs(q *Query, updater docUpdater) error {
	c, ok := s.collections[q.collection]
	if !ok {
		return ErrCollectionNotExist
	}

	docs := make([]*Document, 0)
	s.iterateDocsSlice(q, func(doc *Document) error {
		if q.satisfy(doc) {
			docs = append(docs, doc)
		}
		return nil
	})

	for _, doc := range docs {
		key := doc.ObjectId()
		if q.satisfy(doc) {
			newDoc := updater(doc)
			if newDoc == nil {
				delete(c, key)
			} else {
				c[key].fields = newDoc.fields
			}
		}
	}

	return nil
}
