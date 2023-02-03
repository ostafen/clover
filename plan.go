package clover

import (
	"sort"

	d "github.com/ostafen/clover/v2/document"
	"github.com/ostafen/clover/v2/index"
	"github.com/ostafen/clover/v2/internal"
	"github.com/ostafen/clover/v2/query"
	"github.com/ostafen/clover/v2/store"
)

type planNode interface {
	SetNext(next planNode)
	NextNode() planNode
	Callback(doc *d.Document) error
	Finish() error
}

type inputNode interface {
	planNode
	Run(tx store.Tx) error
}

type planNodeBase struct {
	next planNode
}

func (nd *planNodeBase) NextNode() planNode {
	return nd.next
}

func (nd *planNodeBase) SetNext(next planNode) {
	nd.next = next
}

func (nd *planNodeBase) CallNext(doc *d.Document) error {
	if nd.next != nil {
		return nd.next.Callback(doc)
	}
	return nil
}

func (nd *planNodeBase) Callback(doc *d.Document) error {
	return nil
}

func (nd *planNodeBase) Finish() error {
	return nil
}

type iterNode struct {
	planNodeBase
	filter     query.Criteria
	collection string

	//vRange     *valueRange
	//index      RangeIndex

	idxQuery index.IndexQuery
	//iterIndexReverse bool
}

func (nd *iterNode) iterateFullCollection(tx store.Tx) error {
	prefix := []byte(getDocumentKeyPrefix(nd.collection))
	return iteratePrefix(prefix, tx, func(item store.Item) error {
		doc, err := d.Decode(item.Value)
		if err != nil {
			return err
		}

		if nd.filter == nil || nd.filter.Satisfy(doc) {
			return nd.CallNext(doc)
		}

		return nil
	})
}

func (nd *iterNode) iterateIndex(tx store.Tx) error {
	iterFunc := func(docId string) error {
		doc, err := getDocumentById(nd.collection, docId, tx)

		if err != nil || doc == nil {
			// doc == nil when index record expires after document record
			return err
		}

		if nd.filter == nil || nd.filter.Satisfy(doc) {
			return nd.CallNext(doc)
		}
		return nil
	}

	err := nd.idxQuery.Run(iterFunc)
	return err
}

func (nd *iterNode) Run(tx store.Tx) error {
	if nd.idxQuery != nil {
		return nd.iterateIndex(tx)
	}
	return nd.iterateFullCollection(tx)
}

func getIndexQueries(q *query.Query, indexes []index.Index) []index.IndexQuery {
	if q.Criteria() == nil || len(indexes) == 0 {
		return nil
	}

	info := make(map[string]*index.IndexInfo)
	for _, idx := range indexes {
		info[idx.Field()] = &index.IndexInfo{
			Field: idx.Field(),
			Type:  idx.Type(),
		}
	}

	c := q.Criteria().Accept(&NotFlattenVisitor{}).(query.Criteria)
	selectedFields := c.Accept(&IndexSelectVisitor{
		Fields: info,
	}).([]*index.IndexInfo)

	if len(selectedFields) == 0 {
		return nil
	}

	indexesMap := make(map[string]index.Index)
	for _, idx := range indexes {
		indexesMap[idx.Field()] = idx
	}

	fieldRanges := c.Accept(NewFieldRangeVisitor([]string{selectedFields[0].Field})).(map[string]*index.Range)

	queries := make([]index.IndexQuery, 0)
	for field, vRange := range fieldRanges {
		queries = append(queries, &index.RangeIndexQuery{
			Range: vRange,
			Idx:   indexesMap[field].(index.RangeIndex),
		})
	}
	return queries
}

func tryToSelectIndex(q *query.Query, indexes []index.Index) (*iterNode, bool) {
	indexQueries := getIndexQueries(q, indexes)
	if len(indexQueries) == 1 {
		outputSorted := false

		idxQuery := indexQueries[0]

		if rangeQuery, ok := idxQuery.(*index.RangeIndexQuery); ok {
			if len(q.SortOptions()) == 1 && q.SortOptions()[0].Field == rangeQuery.Idx.Field() {
				rangeQuery.Reverse = q.SortOptions()[0].Direction < 0
				outputSorted = true
			}
		}

		return &iterNode{
			idxQuery:   idxQuery,
			filter:     q.Criteria(),
			collection: q.Collection(),
		}, outputSorted
	}

	if len(q.SortOptions()) == 1 {
		for _, idx := range indexes {
			if idx.Type() == index.IndexSingleField && idx.Field() == q.SortOptions()[0].Field {
				return &iterNode{
					filter:     q.Criteria(),
					collection: q.Collection(),
					idxQuery: &index.RangeIndexQuery{
						Range:   nil,
						Idx:     idx.(index.RangeIndex),
						Reverse: q.SortOptions()[0].Direction < 0,
					},
				}, true
			}
		}
	}
	return nil, false
}

type skipLimitNode struct {
	planNodeBase
	skipped  int
	consumed int
	skip     int
	limit    int
}

func (nd *skipLimitNode) Callback(doc *d.Document) error {
	if nd.skipped < nd.skip {
		nd.skipped++
		return nil
	}

	if nd.limit < 0 || (nd.limit >= 0 && nd.consumed < nd.limit) {
		nd.consumed++
		return nd.CallNext(doc)
	}
	return internal.ErrStopIteration
}

type sortNode struct {
	planNodeBase
	opts []query.SortOption
	docs []*d.Document
}

func (nd *sortNode) Callback(doc *d.Document) error {
	if nd.docs == nil {
		nd.docs = make([]*d.Document, 0)
	}
	nd.docs = append(nd.docs, doc)
	return nil
}

func (nd *sortNode) Finish() error {
	if nd.docs != nil {
		sort.Slice(nd.docs, func(i, j int) bool {
			return compareDocuments(nd.docs[i], nd.docs[j], nd.opts) < 0
		})

		for _, doc := range nd.docs {
			nd.CallNext(doc)
		}
	}
	return nil
}

func buildQueryPlan(q *query.Query, indexes []index.Index, outputNode planNode) inputNode {
	var inputNode inputNode
	var prevNode planNode

	itNode, isOutputSorted := tryToSelectIndex(q, indexes)
	if itNode == nil {
		itNode = &iterNode{
			filter:     q.Criteria(),
			collection: q.Collection(),
		}
	}
	inputNode = itNode
	prevNode = itNode

	//isOutputSorted := (len(q.sortOpts) == 1 && itNode.index != nil && itNode.index.Field() == q.sortOpts[0].Field)
	if len(q.SortOptions()) > 0 && !isOutputSorted {
		nd := &sortNode{opts: q.SortOptions()}
		prevNode.SetNext(nd)
		prevNode = nd
	}

	//log.Println("output sorted: ", len(q.SortOptions()) > 0 && !isOutputSorted)

	if q.GetSkip() > 0 || q.GetLimit() >= 0 {
		nd := &skipLimitNode{skipped: 0, consumed: 0, skip: q.GetSkip(), limit: q.GetLimit()}
		prevNode.SetNext(nd)
		prevNode = nd
	}

	prevNode.SetNext(outputNode)

	return inputNode
}

func execPlan(nd inputNode, tx store.Tx) error {
	if err := nd.Run(tx); err != nil {
		return err
	}

	for curr := nd.(planNode); curr != nil; curr = curr.NextNode() {
		if err := curr.Finish(); err != nil {
			return err
		}
	}
	return nil
}

type consumerNode struct {
	planNodeBase
	consumer docConsumer
}

func (nd *consumerNode) Callback(doc *d.Document) error {
	return nd.consumer(doc)
}

func compareDocuments(first *d.Document, second *d.Document, sortOpts []query.SortOption) int {
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
