package clover

import (
	"bufio"
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

type StorageEngine interface {
	Open(path string) error
	Close() error

	CreateCollection(name string) error
	DropCollection(name string) error
	HasCollection(name string) (bool, error)
	FindAll(q *Query) ([]*Document, error)
	FindById(collectionName string, id string) (*Document, error)
	DeleteById(collectionName string, id string) error
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

type docPointer struct {
	offset uint64
	size   uint32
}

type collection struct {
	name  string
	file  *collectionFile
	index map[string]docPointer
}

type storageImpl struct {
	lock        sync.RWMutex
	path        string
	collections map[string]*collection
}

func newStorageImpl() *storageImpl {
	return &storageImpl{
		collections: make(map[string]*collection),
	}
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
		collFile, err := readCollection(filepath.Join(path, filename))
		if err != nil {
			return err
		}
		collectionName := getBasename(filename)
		s.collections[collectionName] = &collection{name: collectionName, file: collFile, index: make(map[string]docPointer)}
	}
	return nil
}

func (s *storageImpl) CreateCollection(name string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, ok := s.collections[name]; ok {
		return ErrCollectionExist
	}

	collFile, err := readCollection(filepath.Join(s.path, name+collectionFileExt))
	if err != nil {
		return err
	}
	s.collections[name] = &collection{name: name, file: collFile, index: make(map[string]docPointer)}
	return nil
}

func (s *storageImpl) DropCollection(name string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	coll, ok := s.collections[name]
	if !ok {
		return ErrCollectionNotExist
	}

	if err := coll.file.Close(); err != nil {
		return err
	}
	if err := os.Remove(coll.file.Name()); err != nil {
		return nil
	}
	delete(s.collections, name)
	return nil
}

func appendDocs(file *collectionFile, docs []*Document) (map[string]docPointer, error) {
	pointers := make(map[string]docPointer)

	writer := bufio.NewWriter(file)
	for _, doc := range docs {
		jsonText, err := json.Marshal(doc.fields)
		if err != nil {
			return nil, err
		}

		n, err := writer.WriteString(string(jsonText) + "\n")
		if err != nil {
			return nil, err
		}

		pointers[doc.ObjectId()] = docPointer{
			offset: file.size,
			size:   uint32(n),
		}
		file.size += uint64(n)
	}
	return pointers, writer.Flush()
}

func (s *storageImpl) FindAll(q *Query) ([]*Document, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	coll, ok := s.collections[q.collection]
	if !ok {
		return nil, ErrCollectionNotExist
	}

	if _, err := coll.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	docs := make([]*Document, 0)
	sc := bufio.NewScanner(coll.file)
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

func readDoc(collectionFile *collectionFile, ptr docPointer) (*Document, error) {
	data := make([]byte, ptr.size)
	if _, err := collectionFile.ReadAt(data, int64(ptr.offset)); err != nil {
		return nil, err
	}
	doc := NewDocument()
	return doc, json.Unmarshal(data, &doc.fields)
}

func (s *storageImpl) FindById(collectionName string, id string) (*Document, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	coll, ok := s.collections[collectionName]
	if !ok {
		return nil, ErrCollectionNotExist
	}

	ptr, ok := coll.index[id]
	if !ok {
		return nil, nil
	}

	return readDoc(coll.file, ptr)
}

func (s *storageImpl) Insert(collection string, docs ...*Document) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	coll, ok := s.collections[collection]
	if !ok {
		return ErrCollectionNotExist
	}

	tempFile, err := ioutil.TempFile(s.path, collection+collectionFileExt)
	if err != nil {
		return err
	}

	if _, err := coll.file.Seek(0, io.SeekStart); err != nil {
		return nil
	}

	if _, err := io.Copy(tempFile, coll.file); err != nil {
		return err
	}

	pointers, err := appendDocs(&collectionFile{File: tempFile, size: 0}, docs)
	if err != nil {
		return err
	}

	newFile, err := replace(coll.file, tempFile)
	if err != nil {
		return err
	}

	coll.file = newFile
	for docId, ptr := range pointers {
		coll.index[docId] = ptr
	}
	return nil
}

type docUpdater func(doc *Document) *Document

func (s *storageImpl) replaceDocs(collection string, update docUpdater) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	coll, ok := s.collections[collection]
	if !ok {
		return ErrCollectionNotExist
	}

	if _, err := coll.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	tempFile, err := ioutil.TempFile(s.path, collection+collectionFileExt)
	if err != nil {
		return err
	}
	writer := bufio.NewWriter(tempFile)

	fileSize := 0
	sc := bufio.NewScanner(coll.file)
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
		if docToSave != nil {
			text, err := json.Marshal(docToSave.fields)
			if err != nil {
				return err
			}

			n, err := writer.WriteString(string(text) + "\n")
			if err != nil {
				return err
			}
			coll.index[doc.ObjectId()] = docPointer{
				offset: uint64(fileSize),
				size:   uint32(n),
			}
			fileSize += n
		} else {
			delete(coll.index, doc.ObjectId())
		}
	}

	if err := writer.Flush(); err != nil {
		return err
	}

	newFile, err := replace(coll.file, tempFile)
	if err != nil {
		return err
	}
	coll.file = newFile
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
	return s.replaceDocs(q.collection, docUpdater)
}

func (s *storageImpl) Delete(q *Query) error {
	docUpdater := func(doc *Document) *Document {
		if q.satisfy(doc) {
			return nil
		}
		return doc
	}
	return s.replaceDocs(q.collection, docUpdater)
}

func (s *storageImpl) DeleteById(collectionName string, id string) error {
	docUpdater := func(doc *Document) *Document {
		if doc.ObjectId() == id {
			return nil
		}
		return doc
	}
	return s.replaceDocs(collectionName, docUpdater)
}

func (s *storageImpl) Close() error {
	for _, coll := range s.collections {
		if err := coll.file.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (s *storageImpl) HasCollection(name string) (bool, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	_, ok := s.collections[name]
	return ok, nil
}
