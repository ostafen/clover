package clover

import (
	"os"
	"encoding/json"
	"bufio"
	"io/ioutil"
	"errors"
)

// Exports a `collection` to JSON format in `exportPath`
func (db *DB) ExportCollection(collectionName string, exportPath string) error{
	exists, err := db.HasCollection(collectionName)
	if err != nil{
		return err
	}
	if !exists{
		return ErrCollectionNotExist
	}

	result, err := db.Query(collectionName).FindAll()
	if err != nil{
		return err
	}

	docs := make([]map[string]interface{}, 0)
	for _, doc := range result{
		docs = append(docs, doc.fields)
	}

	jsonString, err := json.Marshal(docs)
	if err != nil{
		return err
	}

	return ioutil.WriteFile(exportPath, jsonString, os.ModePerm)
}

// Creates a new `collection` from a JSON file specified by `importPath`
func (db *DB) ImportCollection(collectionName string, importPath string) error {
	file, err := os.Open(importPath)
	if err != nil {
		return err
	}

	if err := db.CreateCollection(collectionName); err != nil {
		return err
	}
	
	reader := bufio.NewReader(file)
	jsonObjects := make([]*map[string]interface{}, 0)
	err = json.NewDecoder(reader).Decode(&jsonObjects)
	if err != nil{
		return err
	}

	docs := make([]*Document, 0)
	for _, doc := range jsonObjects{
		docs = append(docs, NewDocumentOf(*doc))
	}
	return db.Insert(collectionName, docs...)
}
