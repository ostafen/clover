package clover

import (
	"strconv"
	"strings"

	"github.com/ostafen/clover/encoding"
)

// Document represents a document as a map.
type Document struct {
	fields map[string]interface{}
}

// ObjectId returns the id of the document, provided that the document belongs to some collection. Otherwise, it returns the empty string.
func (doc *Document) ObjectId() string {
	id := doc.Get(objectIdField)
	if id == nil {
		return ""
	}
	return id.(string)
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
	normalized, _ := encoding.Normalize(o)
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
		fields: copyMap(doc.fields),
	}
}

func lookupField(name string, fieldMap map[string]interface{}, force bool) (map[string]interface{}, interface{}, string) {
	fields := strings.Split(name, ".")

	var exists bool
	var f interface{}
	currMap := fieldMap
	end := len(fields)
	i := 0
	for i < end {
		field := fields[i]
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

		s, isSlice := f.([]interface{})
		if isSlice {
			if i < end-1 {
				v, increment := lookupSliceField(fields[i+1:], 1, s)
				if v == nil {
					return nil, nil, ""
				} else if m, isMap := v.(map[string]interface{}); isMap {
					currMap = m
				} else {
					f = v
				}

				i += increment
			}
		} else if i < end-1 {
			currMap = m
		}

		i += 1
	}
	return currMap, f, fields[len(fields)-1]
}

func lookupSliceField(fields []string, increment int, s []interface{}) (interface{}, int) {
	fidx := fields[0]
	idx, err := strconv.Atoi(fidx)
	if err != nil {
		return nil, 1
	}
	if idx > len(s)-1 {
		return nil, 1
	}

	value, isSlice := s[idx].([]interface{})
	if isSlice {
		return lookupSliceField(fields[1:], increment+1, value)
	}

	return s[idx], increment

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
	normalizedValue, err := encoding.Normalize(value)
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

// Unmarshal stores the document in the value pointed by v.
func (doc *Document) Unmarshal(v interface{}) error {
	return encoding.Convert(doc.fields, v)
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
			res := compareValues(first.Get(field), second.Get(field))
			if res != 0 {
				return res * direction
			}
		}
	}
	return 0
}
