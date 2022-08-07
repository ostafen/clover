package query

import d "github.com/ostafen/clover/v2/document"

// Query represents a generic query which is submitted to a specific collection.
type Query struct {
	collection string
	criteria   Criteria
	limit      int
	skip       int
	sortOpts   []SortOption
}

// Query simply returns the collection with the supplied name. Use it to initialize a new query.
func NewQuery(collection string) *Query {
	return &Query{
		collection: collection,
		criteria:   nil,
		limit:      -1,
		skip:       0,
		sortOpts:   nil,
	}
}

func (q *Query) copy() *Query {
	return &Query{
		collection: q.collection,
		criteria:   q.criteria,
		limit:      q.limit,
		skip:       q.skip,
		sortOpts:   q.sortOpts,
	}
}

func (q *Query) satisfy(doc *d.Document) bool {
	if q.criteria == nil {
		return true
	}
	return q.criteria.Satisfy(doc)
}

// MatchFunc selects all the documents which satisfy the supplied predicate function.
func (q *Query) MatchFunc(p func(doc *d.Document) bool) *Query {
	return q.Where(newCriteria(FunctionOp, "", p))
}

// Where returns a new Query which select all the documents fullfilling the provided Criteria.
func (q *Query) Where(c Criteria) *Query {
	newQuery := q.copy()
	newQuery.criteria = c
	return newQuery
}

// Skips sets the query so that the first n documents of the result set are discarded.
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
		opts = []SortOption{{Field: d.ObjectIdField, Direction: 1}}
	} else {
		opts = normalizeSortOptions(opts)
	}

	newQuery := q.copy()
	newQuery.sortOpts = opts
	return newQuery
}

func (q *Query) Collection() string {
	return q.collection
}

func (q *Query) Criteria() Criteria {
	return q.criteria
}

func (q *Query) GetLimit() int {
	return q.limit
}

func (q *Query) GetSkip() int {
	return q.skip
}

func (q *Query) SortOptions() []SortOption {
	return q.sortOpts
}
