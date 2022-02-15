package clover

// Query represents a generic query which is submitted to a specific collection.
type Query struct {
	collection *collection
	criteria   *Criteria
}

func (q *Query) satisfy(doc *Document) bool {
	if q.criteria == nil {
		return true
	}
	return q.criteria.p(doc)
}

// Count returns the number of documents which satisfy the query (i.e. len(q.FindAll()) == q.Count()).
func (q *Query) Count() int {
	n := 0
	for _, doc := range q.collection.docs {
		if q.satisfy(doc) {
			n++
		}
	}
	return n
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
		collection: q.collection,
		criteria:   newCriteria,
	}
}

// FindById returns the document with the given id, if such a document exists and satisfies the underlying query, or null.
func (q *Query) FindById(id string) *Document {
	doc, ok := q.collection.docs[id]
	if ok && q.satisfy(doc) {
		return doc
	}
	return nil
}

// FindAll selects all the documents satisfying q.
func (q *Query) FindAll() []*Document {
	docs := make([]*Document, 0)
	for _, doc := range q.collection.docs {
		if q.satisfy(doc) {
			docs = append(docs, doc)
		}
	}
	return docs
}

// Update updates all the document selected by q using the provided updateMap.
// Each update is specified by a mapping fieldName -> newValue.
func (q *Query) Update(updateMap map[string]interface{}) error {
	for _, doc := range q.collection.docs {
		if q.criteria.p(doc) {
			updateDoc := doc.Copy()
			for updateField, updateValue := range updateMap {
				updateDoc.Set(updateField, updateValue)
			}
			q.collection.docs[updateDoc.Get(objectIdField).(string)] = updateDoc
		}
	}
	return q.collection.db.save(q.collection)
}

// DeleteById removes the document with the given id from the underlying collection, provided that such a document exists and satisfies the underlying query.
func (q *Query) DeleteById(id string) error {
	doc, ok := q.collection.docs[id]
	if ok && q.satisfy(doc) {
		delete(q.collection.docs, doc.Get(objectIdField).(string))
		return q.collection.db.save(q.collection)
	}
	return nil
}

// Delete removes all the documents selected by q from the underlying collection.
func (q *Query) Delete() error {
	for _, doc := range q.collection.docs {
		if q.satisfy(doc) {
			delete(q.collection.docs, doc.Get(objectIdField).(string))
		}
	}
	return q.collection.db.save(q.collection)
}
