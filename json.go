package clover

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	d "github.com/ostafen/clover/v2/document"
	"github.com/ostafen/clover/v2/query"
)

// ExportCollection exports an existing collection to a JSON file.
func (db *DB) ExportCollection(collectionName string, exportPath string) error {
	exists, err := db.HasCollection(collectionName)
	if err != nil {
		return err
	}
	if !exists {
		return ErrCollectionNotExist
	}
	q := query.NewQuery(collectionName)

	f, err := os.Create(exportPath)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.WriteString("["); err != nil {
		return err
	}

	var internalErr error
	isFirst := true
	forEachError := db.ForEach(q, func(doc *d.Document) bool {
		jsonByte, err := json.Marshal(doc.AsMap())
		if err != nil {
			internalErr = err
			return false
		}
		jsonString := string(jsonByte)
		if isFirst {
			isFirst = false
		} else {
			jsonString = "," + jsonString
		}
		if _, err := f.WriteString(jsonString); err != nil {
			internalErr = err
			return false
		}
		return true
	})
	exportErrorLog := ""
	if forEachError != nil {
		exportErrorLog += fmt.Sprintf("Export JSON file failed, Error from ForEach [%s]", forEachError.Error())
	}
	if internalErr != nil {
		exportErrorLog += fmt.Sprintf(", Error from internal [%s]", internalErr.Error())
	}
	if exportErrorLog != "" {
		return errors.New(exportErrorLog)
	}

	if _, err := f.WriteString("]"); err != nil {
		return err
	}
	return nil
}

// ImportCollection imports a collection from a JSON file.
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
	if err != nil {
		return err
	}

	docs := make([]*d.Document, 0)
	for _, doc := range jsonObjects {
		docs = append(docs, d.NewDocumentOf(*doc))
	}
	return db.Insert(collectionName, docs...)
}
