package clover

import (
	"bufio"
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"sync"
)

type StorageEngine interface {
	Open(path string) error
	Close() error

	CreateCollection(name string) error
	DropCollection(name string) error
	FindAll(q *Query) ([]*Document, error)
	Insert(collection string, docs ...*Document) error
	Update(q *Query, updateMap map[string]interface{}) error
	Delete(q *Query) error
}

const collectionFileExt = ".coll"

type collectionFile struct {
	*os.File
	size uint64
}

func replace(oldFile *collectionFile, newFile *os.File) (*collectionFile, error) {
	if err := newFile.Close(); err != nil {
		return nil, err
	}
	if err := os.Rename(newFile.Name(), oldFile.Name()); err != nil {
		return nil, err
	}
	return readCollection(oldFile.Name())
}

type storageImpl struct {
	lock        sync.RWMutex
	path        string
	collections map[string]*collectionFile
}

func readCollection(filename string) (*collectionFile, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	stat, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}
	return &collectionFile{File: file, size: uint64(stat.Size())}, err
}

func (s *storageImpl) Open(path string) error {
	s.path = path
	filenames, err := listDir(path)
	if err != nil {
		return err
	}

	for _, filename := range filenames {
		collectionName := getBasename(filename)
		collFile, err := readCollection(filename)
		if err != nil {
			return err
		}
		s.collections[collectionName] = collFile
	}
	return nil
}

func (s *storageImpl) CreateCollection(name string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, ok := s.collections[name]; ok {
		return ErrCollectionExist
	}

	collFile, err := readCollection(name)
	if err != nil {
		return err
	}
	s.collections[name] = collFile
	return nil
}

func (s *storageImpl) DropCollection(name string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	collFile, ok := s.collections[name]
	if !ok {
		return ErrCollectionNotExist
	}

	if err := collFile.Close(); err != nil {
		return err
	}
	if err := os.Remove(collFile.Name()); err != nil {
		return nil
	}
	delete(s.collections, name)
	return nil
}

func appendDocs(file *collectionFile, docs []*Document) error {
	writer := bufio.NewWriter(file)
	for _, doc := range docs {
		jsonText, err := json.Marshal(doc.fields)
		if err != nil {
			return err
		}

		n, err := writer.WriteString(string(jsonText) + "\n")
		if err != nil {
			return err
		}
		file.size += uint64(n)
	}
	return file.Sync()
}

func (s *storageImpl) FindAll(q *Query) ([]*Document, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	collFile, ok := s.collections[q.collection.name]
	if !ok {
		return nil, ErrCollectionNotExist
	}

	if _, err := collFile.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	docs := make([]*Document, 0)
	sc := bufio.NewScanner(collFile)
	for sc.Scan() {
		if sc.Err() != nil {
			return nil, sc.Err()
		}

		jsonText := sc.Text()
		doc := NewDocument()
		if err := json.Unmarshal([]byte(jsonText), &doc.fields); err != nil {
			return nil, err
		}

		if q.satisfy(doc) {
			docs = append(docs, doc)
		}
	}
	return docs, nil
}

func (s *storageImpl) Insert(collection string, docs ...*Document) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	collFile, ok := s.collections[collection]
	if !ok {
		return ErrCollectionNotExist
	}

	tempFile, err := ioutil.TempFile(s.path, collection+collectionFileExt)
	if err != nil {
		return err
	}

	if _, err := collFile.Seek(0, io.SeekStart); err != nil {
		return nil
	}

	if _, err := io.Copy(tempFile, collFile); err != nil {
		return err
	}

	if err := appendDocs(collFile, docs); err != nil {
		return nil
	}

	newFile, err := replace(collFile, tempFile)
	if err != nil {
		return err
	}
	s.collections[collection] = newFile
	return nil
}

type docUpdater func(doc *Document) *Document

func (s *storageImpl) replaceDocs(collection string, update docUpdater) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	collFile, ok := s.collections[collection]
	if !ok {
		return ErrCollectionNotExist
	}

	if _, err := collFile.Seek(0, io.SeekStart); err != nil {
		return err
	}

	tempFile, err := ioutil.TempFile(s.path, collection+collectionFileExt)
	if err != nil {
		return err
	}
	writer := bufio.NewWriter(tempFile)

	sc := bufio.NewScanner(collFile)
	for sc.Scan() {
		if sc.Err() != nil {
			return sc.Err()
		}

		jsonText := sc.Text()
		doc := NewDocument()
		if err := json.Unmarshal([]byte(jsonText), &doc.fields); err != nil {
			return err
		}

		docToSave := update(doc)
		if doc != nil {
			text, err := json.Marshal(docToSave.fields)
			if err != nil {
				return err
			}

			if _, err := writer.WriteString(string(text) + "\n"); err != nil {
				return err
			}
		}
	}

	newFile, err := replace(collFile, tempFile)
	if err != nil {
		return err
	}
	s.collections[collection] = newFile
	return nil
}

func (s *storageImpl) Update(q *Query, updateMap map[string]interface{}) error {
	docUpdater := func(doc *Document) *Document {
		if q.satisfy(doc) {
			updateDoc := doc.Copy()
			for updateField, updateValue := range updateMap {
				updateDoc.Set(updateField, updateValue)
			}
			return updateDoc
		}
		return doc
	}
	return s.replaceDocs(q.collection.name, docUpdater)
}

func (s *storageImpl) Delete(q *Query) error {
	docUpdater := func(doc *Document) *Document {
		if q.satisfy(doc) {
			return nil
		}
		return doc
	}
	return s.replaceDocs(q.collection.name, docUpdater)
}
