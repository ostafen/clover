package clover

import (
	"bytes"
	"fmt"
	"time"

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

func (idx *indexImpl) Set(txn *badger.Txn, value interface{}, docId string, ttl time.Duration) error {
	if ttl == 0 {
		return nil
	}

	encodedKey, err := idx.encodeValueAndId(value, docId)
	if err != nil {
		return err
	}

	e := badger.NewEntry(encodedKey, nil)
	if ttl > 0 {
		e = e.WithTTL(ttl)
	}
	return txn.SetEntry(e)
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

func (idx *indexImpl) IterateRange(txn *badger.Txn, vRange *valueRange, reverse bool, onValue func(docId string) error) error {
	if vRange.isEmpty() {
		return nil
	}

	startKey, endKey, err := idx.encodeRange(vRange)
	if err != nil {
		return err
	}

	seekPrefix := startKey

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false

	if reverse {
		seekPrefix = endKey
		opts.Reverse = true
	}

	if seekPrefix == nil {
		seekPrefix = idx.getKeyPrefix()
	}

	it := txn.NewIterator(opts)
	defer it.Close()

	it.Seek(seekPrefix)

	if !reverse {
		if vRange.start != nil && !vRange.startIncluded { // skip all values equals to range.start
			for ; it.ValidForPrefix(startKey); it.Next() {
			}
		}
	} else {
		if vRange.end != nil && !vRange.endIncluded { // skip all values equals to range.end
			for ; it.ValidForPrefix(endKey); it.Next() {
			}
		}
	}

	prefix := idx.getKeyPrefix()
	for ; it.ValidForPrefix(prefix); it.Next() {
		key := it.Item().Key()

		p, docId := extractDocId(key)

		if !reverse {
			endCmp := bytes.Compare(p, endKey)
			if (vRange.end != nil || vRange.isNil()) && (endCmp > 0 || (endCmp == 0 && !vRange.endIncluded)) {
				break
			}
		} else {
			startCmp := bytes.Compare(p, startKey)
			if (vRange.start != nil || vRange.isNil()) && (startCmp < 0 || (startCmp == 0 && !vRange.startIncluded)) {
				break
			}
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

func (idx *indexImpl) Iterate(txn *badger.Txn, reverse bool, onValue func(docId string) error) error {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Reverse = reverse

	it := txn.NewIterator(opts)
	defer it.Close()

	prefix := idx.getKeyPrefix()
	for it.Seek(prefix); it.Valid(); it.Next() {
		key := it.Item().Key()

		_, docId := extractDocId(key)
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
