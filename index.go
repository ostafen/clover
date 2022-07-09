package clover

import (
	"bytes"
	"fmt"

	badger "github.com/dgraph-io/badger/v3"
	"github.com/ostafen/clover/internal"
)

type indexImpl struct {
	collectionName string
	fieldName      string
}

func (idx *indexImpl) getKeyPrefix(typeId int) []byte {
	return []byte(fmt.Sprintf("c:%s;i:%s;t:%d;v:", idx.collectionName, idx.fieldName, typeId))
}

func extractDocId(key []byte) ([]byte, []byte) {
	if len(key) < 36 {
		panic(string(key))
	}
	return key[:len(key)-36], key[len(key)-36:]
}

func (idx *indexImpl) getKey(v interface{}) ([]byte, error) {
	prefix := idx.getKeyPrefix(internal.TypeId(v))
	return encode(prefix, v)
}

func (idx *indexImpl) lowestKeyPrefix() []byte {
	return idx.getKeyPrefix(0)
}

func (idx *indexImpl) encodeValueAndId(value interface{}, docId string) ([]byte, error) {
	encodedKey, err := idx.getKey(value)
	if err != nil {
		return nil, err
	}
	encodedKey = append(encodedKey, []byte(docId)...)
	return encodedKey, nil
}

func (idx *indexImpl) Set(txn *badger.Txn, value interface{}, docId string) error {
	encodedKey, err := idx.encodeValueAndId(value, docId)
	if err != nil {
		return err
	}
	return txn.Set(encodedKey, nil)
}

func (idx *indexImpl) Delete(txn *badger.Txn, value interface{}, docId string) error {
	encodedKey, err := idx.encodeValueAndId(value, docId)
	if err != nil {
		return err
	}
	return txn.Delete(encodedKey)
}

func (idx *indexImpl) deleteAll(txn *badger.Txn) error {
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	prefix := []byte(fmt.Sprintf("c:%s;i:%s;", idx.collectionName, idx.fieldName))
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		key := it.Item().Key()
		if err := txn.Delete(key); err != nil {
			return err
		}
	}
	return nil
}

func (idx *indexImpl) encodeRange(vRange *valueRange) ([]byte, []byte, error) {
	var err error
	var startKey, endKey []byte

	if vRange.start != nil {
		startKey, err = idx.getKey(vRange.start)
		if err != nil {
			return nil, nil, err
		}
	} else {
		startKey = idx.lowestKeyPrefix()
	}

	if vRange.end != nil {
		var err error
		endKey, err = idx.getKey(vRange.end)
		if err != nil {
			return nil, nil, err
		}
	}
	return startKey, endKey, nil
}

func (idx *indexImpl) Iterate(txn *badger.Txn, vRange *valueRange, onValue func(docId string) error) error {
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	startKey, endKey, err := idx.encodeRange(vRange)
	if err != nil {
		return err
	}

	it.Seek(startKey)

	if vRange.start != nil && !vRange.includeStart { // skip all values equals to first range.start
		for ; it.ValidForPrefix(startKey); it.Next() {
		}
	}

	prefix := []byte(fmt.Sprintf("c:%s;i:%s;", idx.collectionName, idx.fieldName))
	for ; it.ValidForPrefix(prefix); it.Next() {
		key := it.Item().Key()

		p, docId := extractDocId(key)

		endCmp := bytes.Compare(p, endKey)
		if vRange.end != nil && !vRange.includeEnd && endCmp == 0 {
			continue
		}

		if vRange.end != nil && endCmp > 0 {
			break
		}

		if err := onValue(string(docId)); err != nil {
			return err
		}
	}
	return nil
}

type indexQuery struct {
	vRange *valueRange
	index  *indexImpl
}

func selectIndex(nd *andQueryNode, indexedFields map[string]*indexImpl) *indexQuery {
	for key, value := range nd.fields {
		index := indexedFields[key]
		if index != nil {
			return &indexQuery{
				vRange: value,
				index:  index,
			}
		}
	}
	return nil
}

// selectIndexes compute a set of indexQuery which is necessary to query to cover the query criteria represented by queryNode,
// when such set exists, nil otherwise
func selectIndexes(nd queryNode, indexedFields map[string]*indexImpl) []*indexQuery {
	switch ndType := nd.(type) {
	case *notQueryNode:
		return nil
	case *andQueryNode:
		selected := selectIndex(ndType, indexedFields)
		if selected == nil {
			return nil
		}
		return []*indexQuery{selected}
	case *binaryQueryNode:
		n1Indexes := selectIndexes(ndType.n1, indexedFields)
		n2Indexes := selectIndexes(ndType.n2, indexedFields)

		if ndType.OpType == LogicalAnd { // select the indexes with the lowest number of queries
			if n1Indexes != nil && len(n1Indexes) < len(n2Indexes) {
				return n1Indexes
			}
			return n2Indexes
		}

		if n1Indexes == nil || n2Indexes == nil {
			return nil
		}

		res := make([]*indexQuery, 0, len(n1Indexes)+len(n2Indexes))
		for _, idx := range n1Indexes {
			res = append(res, idx)
		}

		for _, idx := range n2Indexes {
			res = append(res, idx)
		}
		return res
	}
	return nil
}
