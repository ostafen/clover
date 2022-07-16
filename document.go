package clover

import (
	"fmt"
	"strings"
	"time"

	"github.com/ostafen/clover/internal"
	"github.com/ostafen/clover/util"
)

// Document represents a document as a map.
type Document struct {
	fields map[string]interface{}
}

// ObjectId returns the id of the document, provided that the document belongs to some collection. Otherwise, it returns the empty string.
func (doc *Document) ObjectId() string {
	id, _ := doc.Get(objectIdField).(string)
	return id
}

// NewDocument creates a new empty document.
func NewDocument() *Document {
	return &Document{
		fields: make(map[string]interface{}),
	}
}

// NewDocumentOf creates a new document and initializes it with the content of the provided object.
// It returns nil if the object cannot be converted to a valid Document.
func NewDocumentOf(o interface{}) *Document {
	normalized, _ := internal.Normalize(o)
	fields, _ := normalized.(map[string]interface{})
	if fields == nil {
		return nil
	}

	return &Document{
		fields: fields,
	}
}

// Copy returns a shallow copy of the underlying document.
func (doc *Document) Copy() *Document {
	return &Document{
		fields: util.CopyMap(doc.fields),
	}
}

func lookupField(name string, fieldMap map[string]interface{}, force bool) (map[string]interface{}, interface{}, string) {
	fields := strings.Split(name, ".")

	var exists bool
	var f interface{}
	currMap := fieldMap
	for i, field := range fields {
		f, exists = currMap[field]

		m, isMap := f.(map[string]interface{})

		if force {
			if (!exists || !isMap) && i < len(fields)-1 {
				m = make(map[string]interface{})
				currMap[field] = m
				f = m
			}
		} else if !exists {
			return nil, nil, ""
		}

		if i < len(fields)-1 {
			currMap = m
		}
	}
	return currMap, f, fields[len(fields)-1]
}

// Has tells returns true if the document contains a field with the supplied name.
func (doc *Document) Has(name string) bool {
	fieldMap, _, _ := lookupField(name, doc.fields, false)
	return fieldMap != nil
}

// Get retrieves the value of a field. Nested fields can be accessed using dot.
func (doc *Document) Get(name string) interface{} {
	_, v, _ := lookupField(name, doc.fields, false)
	return v
}

// Set maps a field to a value. Nested fields can be accessed using dot.
func (doc *Document) Set(name string, value interface{}) {
	normalizedValue, err := internal.Normalize(value)
	if err == nil {
		m, _, fieldName := lookupField(name, doc.fields, true)
		m[fieldName] = normalizedValue
	}
}

// SetAll sets each field specified in the input map to the corresponding value. Nested fields can be accessed using dot.
func (doc *Document) SetAll(values map[string]interface{}) {
	for updateField, updateValue := range values {
		doc.Set(updateField, updateValue)
	}
}

// ExpiresAt returns the document expiration instant
func (doc *Document) ExpiresAt() *time.Time {
	exp, _ := doc.Get(expiresAtField).(*time.Time)
	return exp
}

// ExpiresAt sets document expiration
func (doc *Document) SetExpiresAt(expiration time.Time) {
	doc.Set(expiresAtField, expiration)
}

// TTL returns a duration representing the time to live of the document before expiration.
// A negative duration means that the document has no expiration, while a zero value represents an already expired document.
func (doc *Document) TTL() time.Duration {
	expiresAt := doc.ExpiresAt()
	if expiresAt == nil {
		return time.Duration(-1)
	}

	now := time.Now()

	if expiresAt.Before(now) { // document already expired
		return time.Duration(0)
	}

	return time.Millisecond * time.Duration(expiresAt.Sub(now).Milliseconds())
}

// Unmarshal stores the document in the value pointed by v.
func (doc *Document) Unmarshal(v interface{}) error {
	return internal.Convert(doc.fields, v)
}

func compareDocuments(first *Document, second *Document, sortOpts []SortOption) int {
	for _, opt := range sortOpts {
		field := opt.Field
		direction := opt.Direction

		firstHas := first.Has(field)
		secondHas := second.Has(field)

		if !firstHas && secondHas {
			return -direction
		}

		if firstHas && !secondHas {
			return direction
		}

		if firstHas && secondHas {
			res := internal.Compare(first.Get(field), second.Get(field))
			if res != 0 {
				return res * direction
			}
		}
	}
	return 0
}

func validateDocument(doc *Document) error {
	if !isValidObjectId(doc.ObjectId()) {
		return fmt.Errorf("invalid _id: %s", doc.ObjectId())
	}

	if doc.Has(expiresAtField) && doc.ExpiresAt() == nil {
		return fmt.Errorf("invalid _expiresAt: %s", doc.Get(expiresAtField))
	}
	return nil
}
