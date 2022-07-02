package clover

import (
	"fmt"
)

// Query represents a generic query which is submitted to a specific collection.
type Query struct {
	engine     StorageEngine
	collection string
	criteria   Criteria
	limit      int
	skip       int
	sortOpts   []SortOption
}

func (q *Query) copy() *Query {
	return &Query{
		engine:     q.engine,
		collection: q.collection,
		criteria:   q.criteria,
		limit:      q.limit,
		skip:       q.skip,
		sortOpts:   q.sortOpts,
	}
}

func (q *Query) satisfy(doc *Document) bool {
	if q.criteria == nil {
		return true
	}
	return q.criteria.Satisfy(doc)
}

// Count returns the number of documents which satisfy the query (i.e. len(q.FindAll()) == q.Count()).
func (q *Query) Count() (int, error) {
	num, err := q.engine.Count(q)
	return num, err
}

// Exists returns true if and only if the query result set is not empty.
func (q *Query) Exists() (bool, error) {
	doc, err := q.FindFirst()
	return doc != nil, err
}

// MatchPredicate selects all the documents which satisfy the supplied predicate function.
func (q *Query) MatchPredicate(p func(doc *Document) bool) *Query {
	return q.Where(newCriterion(FunctionOp, "", p))
}

// Where returns a new Query which select all the documents fullfilling both the base query and the provided Criteria.
func (q *Query) Where(c Criteria) *Query {
	newCriteria := q.criteria
	if newCriteria == nil {
		newCriteria = c
	} else {
		newCriteria = newCriteria.And(c)
	}

	newQuery := q.copy()
	newQuery.criteria = newCriteria
	return newQuery
}

func (q *Query) Skip(n int) *Query {
	if n >= 0 {
		newQuery := q.copy()
		newQuery.skip = n
		return newQuery
	}
	return q
}

// Limit sets the query q to consider at most n records.
// As a consequence, the FindAll() method will output at most n documents,
// and any integer m returned by Count() will satisfy the condition m <= n.
func (q *Query) Limit(n int) *Query {
	newQuery := q.copy()
	newQuery.limit = n
	return newQuery
}

// SortOption is used to specify sorting options to the Sort method.
// It consists of a field name and a sorting direction (1 for ascending and -1 for descending).
// Any other positive of negative value (except from 1 and -1) will be equivalent, respectively, to 1 or -1.
// A direction value of 0 (which is also the default value) is assumed to be ascending.
type SortOption struct {
	Field     string
	Direction int
}

func normalizeSortOptions(opts []SortOption) []SortOption {
	normOpts := make([]SortOption, 0, len(opts))
	for _, opt := range opts {
		if opt.Direction >= 0 {
			normOpts = append(normOpts, SortOption{Field: opt.Field, Direction: 1})
		} else {
			normOpts = append(normOpts, SortOption{Field: opt.Field, Direction: -1})
		}
	}
	return normOpts
}

// Sort sets the query so that the returned documents are sorted according list of options.
func (q *Query) Sort(opts ...SortOption) *Query {
	if len(opts) == 0 { // by default, documents are sorted documents by "_id" field
		opts = []SortOption{{Field: objectIdField, Direction: 1}}
	} else {
		opts = normalizeSortOptions(opts)
	}

	newQuery := q.copy()
	newQuery.sortOpts = opts
	return newQuery
}

// FindById returns the document with the given id, if such a document exists and satisfies the underlying query, or null.
func (q *Query) FindById(id string) (*Document, error) {
	return q.engine.FindById(q.collection, id)
}

// FindAll selects all the documents satisfying q.
func (q *Query) FindAll() ([]*Document, error) {
	return q.engine.FindAll(q)
}

// FindFirst returns the first document (if any) satisfying the query.
func (q *Query) FindFirst() (*Document, error) {
	docs, err := q.Limit(1).FindAll()

	var doc *Document
	if len(docs) > 0 {
		doc = docs[0]
	}
	return doc, err
}

// ForEach runs the consumer function for each document matching the provied query.
// If false is returned from the consumer function, then the iteration is stopped.
func (q *Query) ForEach(consumer func(_ *Document) bool) error {
	return q.engine.IterateDocs(q, func(doc *Document) error {
		if !consumer(doc) {
			return errStopIteration
		}
		return nil
	})
}

// Update updates all the document selected by q using the provided updateMap.
// Each update is specified by a mapping fieldName -> newValue.
func (q *Query) Update(updateMap map[string]interface{}) error {
	return q.engine.Update(q, func(doc *Document) *Document {
		newDoc := doc.Copy()
		newDoc.SetAll(updateMap)
		return newDoc
	})
}

// UpdateById updates the document with the specified id using the supplied update map.
// If no document with the specified id exists, an ErrDocumentNotExist is returned.
func (q *Query) UpdateById(docId string, updateMap map[string]interface{}) error {
	return q.engine.UpdateById(q.collection, docId, func(doc *Document) *Document {
		newDoc := doc.Copy()
		newDoc.SetAll(updateMap)
		return newDoc
	})
}

// ReplaceById replaces the document with the specified id with the one provided.
// If no document exists, an ErrDocumentNotExist is returned.
func (q *Query) ReplaceById(docId string, doc *Document) error {
	if doc.ObjectId() != docId {
		return fmt.Errorf("the id of the document must match the one supplied")
	}
	return q.engine.UpdateById(q.collection, docId, func(_ *Document) *Document {
		return doc
	})
}

// DeleteById removes the document with the given id from the underlying collection, provided that such a document exists and satisfies the underlying query.
func (q *Query) DeleteById(id string) error {
	return q.engine.DeleteById(q.collection, id)
}

// Delete removes all the documents selected by q from the underlying collection.
func (q *Query) Delete() error {
	return q.engine.Delete(q)
}
