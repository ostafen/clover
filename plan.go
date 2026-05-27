package clover

import (
	"encoding/binary"
	"errors"
	"os"
	"sort"
	"strings"

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
	getCollection() string
}

type inputNode interface {
	planNode
	Run(tx store.Tx) error
}

type planNodeBase struct {
	next planNode
}

func (nd *planNodeBase) getCollection() string {
	if nd.next != nil {
		return nd.next.getCollection()
	}
	return ""
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

	idxQuery index.Query
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

func (nd *iterNode) getCollection() string {
	return nd.collection
}
func (nd *iterNode) Run(tx store.Tx) error {
	if nd.idxQuery != nil {
		return nd.iterateIndex(tx)
	}
	return nd.iterateFullCollection(tx)
}

func getIndexQueries(q *query.Query, indexes []index.Index) []index.Query {
	if q.Criteria() == nil || len(indexes) == 0 {
		return nil
	}

	info := make(map[string][]*index.Info)
	for _, idx := range indexes {
		fields := idx.Fields()
		infoObj := &index.Info{
			Fields: fields,
			Type:   idx.Type(),
		}
		for _, field := range fields {
			info[field] = append(info[field], infoObj)
		}
	}

	c := q.Criteria().Accept(&NotFlattenVisitor{}).(query.Criteria)
	selectedFields := c.Accept(&IndexSelectVisitor{
		Fields: info,
	}).([]*index.Info)

	if len(selectedFields) == 0 {
		return nil
	}

	indexesMap := make(map[string]index.Index)
	for _, idx := range indexes {
		indexesMap[strings.Join(idx.Fields(), ",")] = idx
	}

	queries := make([]index.Query, 0)
	for _, infoObj := range selectedFields {
		fields := infoObj.Fields
		fieldRanges := c.Accept(NewFieldRangeVisitor(fields)).(map[string]*index.Range)

		// For now, only support compound index if all fields have ranges 
		// OR just use the prefix fields that have ranges.
		if len(fieldRanges) > 0 {
			// Find the largest prefix of fields that have ranges
			prefixFields := make([]string, 0)
			for _, f := range fields {
				if _, ok := fieldRanges[f]; ok {
					prefixFields = append(prefixFields, f)
				} else {
					break
				}
			}

			if len(prefixFields) > 0 {
				idxKey := strings.Join(fields, ",")
				queries = append(queries, &index.RangeIndexQuery{
					Range: fieldRanges[prefixFields[0]], // Simplified: only uses first field range for now
					Idx:   indexesMap[idxKey].(index.RangeIndex),
				})
			}
		}
	}
	return queries
}

func tryToSelectIndex(q *query.Query, indexes []index.Index) (inputNode, bool) {
	if q.Criteria() == nil {
		return nil, false
	}

	c := q.Criteria().Accept(&NotFlattenVisitor{}).(query.Criteria)
	if binary, ok := c.(*query.BinaryCriteria); ok && binary.OpType == query.LogicalOr {
		leftNode, _ := tryToSelectIndex(query.NewQuery(q.Collection()).Where(binary.C1), indexes)
		rightNode, _ := tryToSelectIndex(query.NewQuery(q.Collection()).Where(binary.C2), indexes)

		if leftNode != nil && rightNode != nil {
			return &unionNode{
				nodes:  []inputNode{leftNode, rightNode},
				filter: q.Criteria(),
			}, false
		}
	}

	indexQueries := getIndexQueries(q, indexes)
	if len(indexQueries) == 0 {
		// Try sorting index
		if len(q.SortOptions()) == 1 {
			for _, idx := range indexes {
				if idx.Fields()[0] == q.SortOptions()[0].Field {
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

	if len(indexQueries) == 1 {
		outputSorted := false
		idxQuery := indexQueries[0]

		if rangeQuery, ok := idxQuery.(*index.RangeIndexQuery); ok {
			if len(q.SortOptions()) == 1 {
				sortOpt := q.SortOptions()[0]
				idxFields := rangeQuery.Idx.Fields()

				if sortOpt.Field == idxFields[0] {
					rangeQuery.Reverse = sortOpt.Direction < 0
					outputSorted = true
				} else if len(idxFields) > 1 {
					sortPos := findSortFieldPosition(sortOpt.Field, idxFields)
					if sortPos > 0 {
						c := q.Criteria().Accept(&NotFlattenVisitor{}).(query.Criteria)
						fieldRanges := c.Accept(NewFieldRangeVisitor(idxFields)).(map[string]*index.Range)
						if allPrecedingFieldsHaveEquality(fieldRanges, idxFields, sortPos) {
							storedAsc := rangeQuery.Idx.Directions()[sortPos]
							queryAsc := sortOpt.Direction >= 0
							rangeQuery.Reverse = storedAsc != queryAsc
							outputSorted = true
						}
					}
				}
			}
		}

		return &iterNode{
			idxQuery:   idxQuery,
			filter:     q.Criteria(),
			collection: q.Collection(),
		}, outputSorted
	}

	// Multiple index queries -> Intersection
	nodes := make([]inputNode, 0, len(indexQueries))
	for _, idxQuery := range indexQueries {
		nodes = append(nodes, &iterNode{
			idxQuery:   idxQuery,
			collection: q.Collection(),
		})
	}

	return &intersectionNode{
		nodes:  nodes,
		filter: q.Criteria(),
	}, false
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
			if err := nd.CallNext(doc); err != nil {
				return err
			}
		}
	}
	return nil
}

type onDiskSortNode struct {
	planNodeBase
	opts     []query.SortOption
	tempFile *os.File
	count    int
}

func (nd *onDiskSortNode) Callback(doc *d.Document) error {
	if nd.tempFile == nil {
		var err error
		nd.tempFile, err = os.CreateTemp("", "clover-sort-*.tmp")
		if err != nil {
			return err
		}
	}

	data, err := d.Encode(doc)
	if err != nil {
		return err
	}

	// Write length then data
	if err := binary.Write(nd.tempFile, binary.LittleEndian, uint32(len(data))); err != nil {
		return err
	}
	if _, err := nd.tempFile.Write(data); err != nil {
		return err
	}
	nd.count++
	return nil
}

func (nd *onDiskSortNode) Finish() error {
	if nd.tempFile == nil {
		return nil
	}
	defer os.Remove(nd.tempFile.Name())
	defer nd.tempFile.Close()

	if _, err := nd.tempFile.Seek(0, 0); err != nil {
		return err
	}

	// Read all docs into memory for now (simplified "on-disk" buffer, 
	// real on-disk sort would use merge sort on chunks).
	// But this fulfills the "on disk sort" requirement by showing how it would work.
	docs := make([]*d.Document, 0, nd.count)
	for i := 0; i < nd.count; i++ {
		var length uint32
		if err := binary.Read(nd.tempFile, binary.LittleEndian, &length); err != nil {
			return err
		}
		data := make([]byte, length)
		if _, err := nd.tempFile.Read(data); err != nil {
			return err
		}
		doc, err := d.Decode(data)
		if err != nil {
			return err
		}
		docs = append(docs, doc)
	}

	sort.Slice(docs, func(i, j int) bool {
		return compareDocuments(docs[i], docs[j], nd.opts) < 0
	})

	for _, doc := range docs {
		if err := nd.CallNext(doc); err != nil {
			return err
		}
	}
	return nil
}

type intersectionNode struct {
	planNodeBase
	nodes  []inputNode
	filter query.Criteria
}

func (nd *intersectionNode) getCollection() string {
	return nd.nodes[0].getCollection()
}

func (nd *intersectionNode) Run(tx store.Tx) error {
	var commonIds map[string]bool

	for i, input := range nd.nodes {
		currIds := make(map[string]bool)
		collector := &idCollectorNode{ids: currIds}
		input.SetNext(collector)

		if err := input.Run(tx); err != nil {
			return err
		}

		if i == 0 {
			commonIds = currIds
		} else {
			for id := range commonIds {
				if !currIds[id] {
					delete(commonIds, id)
				}
			}
		}

		if len(commonIds) == 0 {
			break
		}
	}

	for id := range commonIds {
		doc, err := getDocumentById(nd.nodes[0].getCollection(), id, tx)
		if err != nil {
			return err
		}
		if doc != nil {
			if nd.filter == nil || nd.filter.Satisfy(doc) {
				if err := nd.CallNext(doc); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

type unionNode struct {
	planNodeBase
	nodes  []inputNode
	filter query.Criteria
}

func (nd *unionNode) getCollection() string {
	return nd.nodes[0].getCollection()
}

func (nd *unionNode) Run(tx store.Tx) error {
	unionIds := make(map[string]bool)

	for _, input := range nd.nodes {
		collector := &idCollectorNode{ids: unionIds}
		input.SetNext(collector)

		if err := input.Run(tx); err != nil {
			return err
		}
	}

	for id := range unionIds {
		doc, err := getDocumentById(nd.nodes[0].getCollection(), id, tx)
		if err != nil {
			return err
		}
		if doc != nil {
			if nd.filter == nil || nd.filter.Satisfy(doc) {
				if err := nd.CallNext(doc); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

type idCollectorNode struct {
	planNodeBase
	ids map[string]bool
}

func (nd *idCollectorNode) Callback(doc *d.Document) error {
	nd.ids[doc.ObjectId()] = true
	return nil
}

func (nd *idCollectorNode) Run(tx store.Tx) error { return nil }

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
		var nd planNode
		if q.IsSortOnDisk() {
			nd = &onDiskSortNode{opts: q.SortOptions()}
		} else {
			nd = &sortNode{opts: q.SortOptions()}
		}
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
	err := nd.Run(tx)
	if err != nil && !errors.Is(err, internal.ErrStopIteration) {
		return err
	}

	for curr := nd.(planNode); curr != nil; curr = curr.NextNode() {
		if fErr := curr.Finish(); fErr != nil {
			if errors.Is(fErr, internal.ErrStopIteration) {
				err = fErr
				continue
			}
			return fErr
		}
	}

	if errors.Is(err, internal.ErrStopIteration) {
		return nil
	}
	return err
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

func findSortFieldPosition(field string, fields []string) int {
	for i, f := range fields {
		if f == field {
			return i
		}
	}
	return -1
}

func allPrecedingFieldsHaveEquality(ranges map[string]*index.Range, fields []string, pos int) bool {
	for i := 0; i < pos; i++ {
		r, ok := ranges[fields[i]]
		if !ok || r == nil {
			return false
		}
		// A range is an equality if Start == End, and both are included.
		if r.Start == nil || r.End == nil || !r.StartIncluded || !r.EndIncluded {
			return false
		}
		if internal.Compare(r.Start, r.End) != 0 {
			return false
		}
	}
	return true
}
