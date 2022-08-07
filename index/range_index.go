package index

import (
	"bytes"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/ostafen/clover/v2/internal"
)

type RangeIndex interface {
	Index
	IterateRange(vRange *Range, reverse bool, onValue func(docId string) error) error
}

type RangeIndexQuery struct {
	Range   *Range
	Reverse bool
	Idx     RangeIndex
}

func (q *RangeIndexQuery) Run(onValue func(docId string) error) error {
	if q.Range == nil {
		return q.Idx.Iterate(q.Reverse, onValue)
	}
	return q.Idx.IterateRange(q.Range, q.Reverse, onValue)
}

type badgerRangeIndex struct {
	indexBase
	txn *badger.Txn
}

func extractDocId(key []byte) ([]byte, []byte) {
	if len(key) < 36 {
		panic(string(key))
	}
	return key[:len(key)-36], key[len(key)-36:]
}

func (idx *badgerRangeIndex) getKeyPrefix() []byte {
	return []byte(fmt.Sprintf("c:%s;i:%s", idx.collection, idx.field))
}

func (idx *badgerRangeIndex) getKeyPrefixForType(typeId int) []byte {
	return []byte(fmt.Sprintf("%s;t:%d;v:", idx.getKeyPrefix(), typeId))
}

func (idx *badgerRangeIndex) getKey(v interface{}) ([]byte, error) {
	prefix := idx.getKeyPrefixForType(internal.TypeId(v))
	return internal.OrderedCode(prefix, v)
}

func (idx *badgerRangeIndex) encodeValueAndId(value interface{}, docId string) ([]byte, error) {
	encodedKey, err := idx.getKey(value)
	if err != nil {
		return nil, err
	}
	encodedKey = append(encodedKey, []byte(docId)...)
	return encodedKey, nil
}

func (idx *badgerRangeIndex) Add(docId string, v interface{}, ttl time.Duration) error {
	if ttl == 0 {
		return nil
	}

	encodedKey, err := idx.encodeValueAndId(v, docId)
	if err != nil {
		return err
	}

	e := badger.NewEntry(encodedKey, nil)
	if ttl > 0 {
		e = e.WithTTL(ttl)
	}
	return idx.txn.SetEntry(e)
}

func (idx *badgerRangeIndex) Remove(docId string, value interface{}) error {
	encodedKey, err := idx.encodeValueAndId(value, docId)
	if err != nil {
		return err
	}
	return idx.txn.Delete(encodedKey)
}

func (idx *badgerRangeIndex) Drop() error {
	it := idx.txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	prefix := idx.getKeyPrefix()
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		key := it.Item().Key()
		if err := idx.txn.Delete(key); err != nil {
			return err
		}
	}
	return nil
}

func (idx *badgerRangeIndex) encodeRange(vRange *Range) ([]byte, []byte, error) {
	var err error
	var startKey, endKey []byte

	if vRange.IsNil() || vRange.Start != nil {
		startKey, err = idx.getKey(vRange.Start)
		if err != nil {
			return nil, nil, err
		}
	}

	if vRange.IsNil() || vRange.End != nil {
		var err error
		endKey, err = idx.getKey(vRange.End)
		if err != nil {
			return nil, nil, err
		}
	}
	return startKey, endKey, nil
}

func (idx *badgerRangeIndex) IterateRange(vRange *Range, reverse bool, onValue func(docId string) error) error {
	if vRange.IsEmpty() {
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
		if reverse {
			seekPrefix = append(seekPrefix, 255)
		}
	}

	it := idx.txn.NewIterator(opts)
	defer it.Close()

	it.Seek(seekPrefix)

	if !reverse {
		if vRange.Start != nil && !vRange.StartIncluded { // skip all values equals to range.start
			for ; it.ValidForPrefix(startKey); it.Next() {
			}
		}
	} else {
		if vRange.End != nil && !vRange.EndIncluded { // skip all values equals to range.end
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
			if (vRange.End != nil || vRange.IsNil()) && (endCmp > 0 || (endCmp == 0 && !vRange.EndIncluded)) {
				break
			}
		} else {
			startCmp := bytes.Compare(p, startKey)
			if (vRange.Start != nil || vRange.IsNil()) && (startCmp < 0 || (startCmp == 0 && !vRange.StartIncluded)) {
				break
			}
		}

		if err := onValue(string(docId)); err != nil {
			if err == internal.ErrStopIteration {
				return nil
			}
			return err
		}
	}
	return nil
}

func (idx *badgerRangeIndex) Iterate(reverse bool, onValue func(docId string) error) error {
	opts := badger.DefaultIteratorOptions
	opts.Reverse = reverse

	it := idx.txn.NewIterator(opts)
	defer it.Close()

	prefix := idx.getKeyPrefix()

	seekPrefix := prefix
	if reverse {
		seekPrefix = append(seekPrefix, 255)
	}

	it.Seek(seekPrefix)

	for ; it.ValidForPrefix(prefix); it.Next() {
		key := it.Item().Key()

		_, docId := extractDocId(key)
		if err := onValue(string(docId)); err != nil {
			if err == internal.ErrStopIteration {
				return nil
			}
			return err
		}
	}
	return nil
}

func (idx *badgerRangeIndex) Type() IndexType {
	return IndexSingleField
}
