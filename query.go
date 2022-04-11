package clover

// Query represents a generic query which is submitted to a specific collection.
type Query struct {
	engine     StorageEngine
	collection string
	criteria   *Criteria
	limit      int
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

	return &Query{
		engine:     q.engine,
		collection: q.collection,
		criteria:   newCriteria,
		limit:      q.limit,
	}
}

// FindById returns the document with the given id, if such a document exists and satisfies the underlying query, or null.
func (q *Query) FindById(id string) (*Document, error) {
	return q.engine.FindById(q.collection, id)
}

// FindAll selects all the documents satisfying q.
func (q *Query) FindAll() ([]*Document, error) {
	return q.engine.FindAll(q)
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
