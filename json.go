package clover

import (
	"bufio"
	"encoding/json"
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

	file, err := os.OpenFile(exportPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	// Stream one document at a time instead of loading the whole collection
	// into memory, so exporting large collections stays cheap.
	if _, err := writer.WriteString("["); err != nil {
		return err
	}

	first := true
	err = db.IterateDocs(query.NewQuery(collectionName), func(doc *d.Document) error {
		jsonBytes, err := json.Marshal(doc.AsMap())
		if err != nil {
			return err
		}
		if !first {
			if _, err := writer.WriteString(","); err != nil {
				return err
			}
		}
		first = false
		_, err = writer.Write(jsonBytes)
		return err
	})
	if err != nil {
		return err
	}

	if _, err := writer.WriteString("]"); err != nil {
		return err
	}

	return writer.Flush()
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
