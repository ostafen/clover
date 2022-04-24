package clover

import (
	"sort"
	"sync"
)

type collection map[string]*Document

type memEngine struct {
	sync.RWMutex
	collections map[string]collection
}

func newMemStorageEngine() StorageEngine {
	return &memEngine{
		RWMutex:     sync.RWMutex{},
		collections: make(map[string]collection),
	}
}

// Close implements StorageEngine
func (*memEngine) Close() error {
	return nil
}

// CreateCollection implements StorageEngine
func (e *memEngine) CreateCollection(name string) error {
	e.Lock()
	defer e.Unlock()
	if e.hasCollection(name) {
		return ErrCollectionExist
	}
	e.collections[name] = make(collection)
	return nil
}

// Delete implements StorageEngine
func (e *memEngine) Delete(q *Query) error {
	return e.replaceDocs(q, func(_ *Document) *Document {
		return nil
	})
}

// DeleteById implements StorageEngine
func (e *memEngine) DeleteById(collectionName string, id string) error {
	e.Lock()
	defer e.Unlock()

	c, ok := e.collections[collectionName]
	if !ok {
		return ErrCollectionNotExist
	}

	delete(c, id)

	return nil
}

func (e *memEngine) hasCollection(collectionName string) bool {
	_, ok := e.collections[collectionName]
	return ok
}

// DropCollection implements StorageEngine
func (e *memEngine) DropCollection(name string) error {
	e.Lock()
	defer e.Unlock()

	if e.hasCollection(name) {
		delete(e.collections, name)
	} else {
		return ErrCollectionNotExist
	}
	return nil
}

// FindAll implements StorageEngine
func (e *memEngine) FindAll(q *Query) ([]*Document, error) {
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
func (e *memEngine) FindById(collectionName string, id string) (*Document, error) {
	e.RLock()
	defer e.RUnlock()

	c, ok := e.collections[collectionName]
	if !ok {
		return nil, ErrCollectionNotExist
	}

	return c[id], nil
}

// HasCollection implements StorageEngine
func (e *memEngine) HasCollection(name string) (bool, error) {
	e.RLock()
	defer e.RUnlock()

	_, ok := e.collections[name]
	return ok, nil
}

// Insert implements StorageEngine
func (e *memEngine) Insert(collection string, docs ...*Document) error {
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

func (e *memEngine) Save(collection string, doc *Document) error {
	e.Lock()
	defer e.Unlock()

	c, ok := e.collections[collection]
	if !ok {
		return ErrCollectionNotExist
	}

	c[doc.ObjectId()] = doc

	return nil
}

func (e *memEngine) iterateDocs(q *Query, consumer docConsumer) error {
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
func (e *memEngine) IterateDocs(q *Query, consumer docConsumer) error {
	e.RLock()
	defer e.RUnlock()

	return e.iterateDocs(q, consumer)
}

// Open implements StorageEngine
func (e *memEngine) Open(path string) error {
	return nil
}

// Update implements StorageEngine
func (e *memEngine) Update(q *Query, updateMap map[string]interface{}) error {
	return e.replaceDocs(q, func(doc *Document) *Document {
		updateDoc := doc.Copy()
		for updateField, updateValue := range updateMap {
			updateDoc.Set(updateField, updateValue)
		}
		return updateDoc
	})
}

func (e *memEngine) replaceDocs(q *Query, updater docUpdater) error {
	e.Lock()
	defer e.Unlock()

	c, ok := e.collections[q.collection]
	if !ok {
		return ErrCollectionNotExist
	}

	docs := make([]*Document, 0)
	e.iterateDocs(q, func(doc *Document) error {
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

func (e *memEngine) ListCollections() ([]string, error) {
	collections := make([]string, 0)
	for name := range e.collections {
		collections = append(collections, name)
	}
	return collections, nil
}
