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

func (idx *indexImpl) getKeyPrefix() []byte {
	return []byte(fmt.Sprintf("c:%s;i:%s;", idx.collectionName, idx.fieldName))
}

func (idx *indexImpl) getKeyPrefixForType(typeId int) []byte {
	return []byte(fmt.Sprintf("%s;t:%d;v:", idx.getKeyPrefix(), typeId))
}

func extractDocId(key []byte) ([]byte, []byte) {
	if len(key) < 36 {
		panic(string(key))
	}
	return key[:len(key)-36], key[len(key)-36:]
}

func (idx *indexImpl) getKey(v interface{}) ([]byte, error) {
	prefix := idx.getKeyPrefixForType(internal.TypeId(v))
	return internal.OrderedCode(prefix, v)
}

func (idx *indexImpl) lowestKeyPrefix() []byte {
	return idx.getKeyPrefixForType(0)
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

	prefix := idx.getKeyPrefix()
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

	if vRange.isNil() || vRange.start != nil {
		startKey, err = idx.getKey(vRange.start)
		if err != nil {
			return nil, nil, err
		}
	} else {
		startKey = idx.lowestKeyPrefix()
	}

	if vRange.isNil() || vRange.end != nil {
		var err error
		endKey, err = idx.getKey(vRange.end)
		if err != nil {
			return nil, nil, err
		}
	}
	return startKey, endKey, nil
}

func (idx *indexImpl) IterateRange(txn *badger.Txn, vRange *valueRange, onValue func(docId string) error) error {
	if vRange.isEmpty() {
		return nil
	}

	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	startKey, endKey, err := idx.encodeRange(vRange)
	if err != nil {
		return err
	}

	it.Seek(startKey)

	if vRange.start != nil && !vRange.startIncluded { // skip all values equals to first range.start
		for ; it.ValidForPrefix(startKey); it.Next() {
		}
	}

	prefix := idx.getKeyPrefix()
	for ; it.ValidForPrefix(prefix); it.Next() {
		key := it.Item().Key()

		p, docId := extractDocId(key)

		endCmp := bytes.Compare(p, endKey)
		if (vRange.end != nil || vRange.isNil()) && (endCmp > 0 || (endCmp == 0 && !vRange.endIncluded)) {
			break
		}

		if err := onValue(string(docId)); err != nil {
			if err == errStopIteration {
				return nil
			}
			return err
		}
	}
	return nil
}

type indexQuery struct {
	vRange *valueRange
	index  *indexImpl
}
