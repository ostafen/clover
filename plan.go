package clover

import (
	"errors"
	"sort"

	"github.com/dgraph-io/badger/v3"
)

type planNode interface {
	SetNext(next planNode)
	NextNode() planNode
	Callback(doc *Document) error
	Finish() error
}

type inputNode interface {
	planNode
	Run(txn *badger.Txn) error
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

func (nd *planNodeBase) CallNext(doc *Document) error {
	if nd.next != nil {
		return nd.next.Callback(doc)
	}
	return nil
}

func (nd *planNodeBase) Callback(doc *Document) error {
	return nil
}

func (nd *planNodeBase) Finish() error {
	return nil
}

type iterNode struct {
	planNodeBase
	filter           Criteria
	collection       string
	vRange           *valueRange
	index            *indexImpl
	iterIndexReverse bool
}

func (nd *iterNode) iterateFullCollection(txn *badger.Txn) error {
	prefix := []byte(getDocumentKeyPrefix(nd.collection))
	return iteratePrefix(prefix, txn, func(item *badger.Item) error {
		return item.Value(func(data []byte) error {
			doc, err := decodeDoc(data)
			if err != nil {
				return err
			}

			if nd.filter == nil || nd.filter.Satisfy(doc) {
				return nd.CallNext(doc)
			}

			return nil
		})
	})
}

func (nd *iterNode) iterateIndex(txn *badger.Txn) error {
	iterFunc := func(docId string) error {
		doc, err := getDocumentById(nd.collection, docId, txn)

		if err != nil {
			// err == badger.ErrKeyNotFound when index record expires after document record
			if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
		}

		if nd.filter == nil || nd.filter.Satisfy(doc) {
			return nd.CallNext(doc)
		}
		return nil
	}

	if nd.vRange != nil {
		return nd.index.IterateRange(txn, nd.vRange, nd.iterIndexReverse, iterFunc)
	}
	return nd.index.Iterate(txn, nd.iterIndexReverse, iterFunc)
}

func (nd *iterNode) Run(txn *badger.Txn) error {
	if nd.index != nil {
		return nd.iterateIndex(txn)
	}
	return nd.iterateFullCollection(txn)
}

func getIndexQueries(q *Query, indexes []*indexImpl) []*indexQuery {
	if q.criteria == nil || len(indexes) == 0 {
		return nil
	}

	indexedFields := make(map[string]bool)
	for _, idx := range indexes {
		indexedFields[idx.fieldName] = true
	}

	c := q.criteria.Accept(&NotFlattenVisitor{}).(Criteria)
	selectedFields := c.Accept(&IndexSelectVisitor{
		Fields: indexedFields,
	}).([]string)

	if len(selectedFields) == 0 {
		return nil
	}

	fieldRanges := c.Accept(NewFieldRangeVisitor(selectedFields)).(map[string]*valueRange)

	indexesMap := make(map[string]*indexImpl)
	for _, idx := range indexes {
		indexesMap[idx.fieldName] = idx
	}

	queries := make([]*indexQuery, 0)
	for field, vRange := range fieldRanges {
		queries = append(queries, &indexQuery{
			vRange: vRange,
			index:  indexesMap[field],
		})
	}

	return queries
}

func tryToSelectIndex(q *Query, indexes []*indexImpl) *iterNode {
	indexQueries := getIndexQueries(q, indexes)
	if len(indexQueries) == 1 {
		nd := &iterNode{
			vRange:     indexQueries[0].vRange,
			index:      indexQueries[0].index,
			filter:     q.criteria,
			collection: q.collection,
		}

		if len(q.sortOpts) == 1 && q.sortOpts[0].Field == indexQueries[0].index.fieldName {
			nd.iterIndexReverse = q.sortOpts[0].Direction < 0
		}
		return nd
	}

	if len(q.sortOpts) == 1 {
		for _, idx := range indexes {
			if idx.fieldName == q.sortOpts[0].Field {
				return &iterNode{
					vRange:           nil,
					index:            idx,
					collection:       q.collection,
					iterIndexReverse: q.sortOpts[0].Direction < 0,
				}
			}
		}
	}
	return nil
}

type skipLimitNode struct {
	planNodeBase
	skipped  int
	consumed int
	skip     int
	limit    int
}

func (nd *skipLimitNode) Callback(doc *Document) error {
	if nd.skipped < nd.skip {
		nd.skipped++
		return nil
	}

	if nd.limit < 0 || (nd.limit >= 0 && nd.consumed < nd.limit) {
		nd.consumed++
		return nd.CallNext(doc)
	}
	return errStopIteration
}

type sortNode struct {
	planNodeBase
	opts []SortOption
	docs []*Document
}

func (nd *sortNode) Callback(doc *Document) error {
	if nd.docs == nil {
		nd.docs = make([]*Document, 0)
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

func buildQueryPlan(q *Query, indexes []*indexImpl, outputNode planNode) inputNode {
	var inputNode inputNode
	var prevNode planNode

	itNode := tryToSelectIndex(q, indexes)
	if itNode == nil {
		itNode = &iterNode{
			filter:     q.criteria,
			collection: q.collection,
		}
	}
	inputNode = itNode
	prevNode = itNode

	isOutputSorted := (len(q.sortOpts) == 1 && itNode.index != nil && itNode.index.fieldName == q.sortOpts[0].Field)
	if len(q.sortOpts) > 0 && !isOutputSorted {
		nd := &sortNode{opts: q.sortOpts}
		prevNode.SetNext(nd)
		prevNode = nd
	}

	if q.skip > 0 || q.limit >= 0 {
		nd := &skipLimitNode{skipped: 0, consumed: 0, skip: q.skip, limit: q.limit}
		prevNode.SetNext(nd)
		prevNode = nd
	}

	prevNode.SetNext(outputNode)

	return inputNode
}

func execPlan(nd inputNode, txn *badger.Txn) error {
	if err := nd.Run(txn); err != nil {
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

func (nd *consumerNode) Callback(doc *Document) error {
	return nd.consumer(doc)
}
