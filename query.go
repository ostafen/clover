package clover

// Query represents a generic query which is submitted to a specific collection.
type Query struct {
	engine     StorageEngine
	collection string
	criteria   *Criteria
	limit      int
	skip       int
}

func (q *Query) copy() *Query {
	return &Query{
		engine:     q.engine,
		collection: q.collection,
		criteria:   q.criteria,
		limit:      q.limit,
		skip:       q.skip,
	}
}

func (q *Query) satisfy(doc *Document) bool {
	if q.criteria == nil {
		return true
	}
	return q.criteria.p(doc)
}

// Count returns the number of documents which satisfy the query (i.e. len(q.FindAll()) == q.Count()).
func (q *Query) Count() (int, error) {
	docs, err := q.FindAll()
	return len(docs), err
}

// MatchPredicate selects all the documents which satisfy the supplied predicate function.
func (q *Query) MatchPredicate(p func(doc *Document) bool) *Query {
	return q.Where(&Criteria{p})
}

// Where returns a new Query which select all the documents fullfilling both the base query and the provided Criteria.
func (q *Query) Where(c *Criteria) *Query {
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
	newQuery := q.copy()
	newQuery.skip = n
	return newQuery
}

// Limit sets the query q to consider at most n records.
// As a consequence, the FindAll() method will output at most n documents,
// and any integer m returned by Count() will satisfy the condition m <= n.
func (q *Query) Limit(n int) *Query {
	newQuery := q.copy()
	newQuery.limit = n
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
	return q.engine.Update(q, updateMap)
}

// DeleteById removes the document with the given id from the underlying collection, provided that such a document exists and satisfies the underlying query.
func (q *Query) DeleteById(id string) error {
	return q.engine.DeleteById(q.collection, id)
}

// Delete removes all the documents selected by q from the underlying collection.
func (q *Query) Delete() error {
	return q.engine.Delete(q)
}
