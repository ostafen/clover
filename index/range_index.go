package index

import (
	"bytes"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/ostafen/clover/v2/internal"
	"github.com/ostafen/clover/v2/store"
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
	tx store.Tx
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
	encodedKey, err := idx.encodeValueAndId(v, docId)
	if err != nil {
		return err
	}
	return idx.tx.Set(encodedKey, nil)
}

func (idx *badgerRangeIndex) Remove(docId string, value interface{}) error {
	encodedKey, err := idx.encodeValueAndId(value, docId)
	if err != nil {
		return err
	}
	return idx.tx.Delete(encodedKey)
}

func (idx *badgerRangeIndex) Drop() error {
	cursor, err := idx.tx.Cursor(true)
	if err != nil {
		return err
	}
	defer cursor.Close()

	prefix := idx.getKeyPrefix()
	cursor.Seek(prefix)
	for ; cursor.Valid(); cursor.Next() {
		item, err := cursor.Item()
		if err != nil {
			return err
		}

		if !bytes.HasPrefix(item.Key, prefix) {
			return nil
		}

		if err := idx.tx.Delete(item.Key); err != nil {
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
	if reverse {
		seekPrefix = endKey
	}

	if seekPrefix == nil {
		seekPrefix = idx.getKeyPrefix()
		if reverse {
			seekPrefix = append(seekPrefix, 255)
		}
	}

	cursor, err := idx.tx.Cursor(!reverse)
	if err != nil {
		return err
	}
	defer cursor.Close()

	cursor.Seek(seekPrefix)

	if !reverse {
		if vRange.Start != nil && !vRange.StartIncluded { // skip all values equals to range.start
			for ; cursor.Valid(); cursor.Next() {
				item, err := cursor.Item()
				if err != nil {
					return err
				}

				if !bytes.HasPrefix(item.Key, startKey) {
					break
				}
			}
		}
	} else {
		if vRange.End != nil && !vRange.EndIncluded { // skip all values equals to range.end
			for ; cursor.Valid(); cursor.Next() {
				item, err := cursor.Item()
				if err != nil {
					return err
				}

				if !bytes.HasPrefix(item.Key, endKey) {
					break
				}
			}
		}
	}

	prefix := idx.getKeyPrefix()
	for ; cursor.Valid(); cursor.Next() {
		item, err := cursor.Item()
		if err != nil {
			return err
		}

		key := item.Key
		if !bytes.HasPrefix(key, prefix) {
			return nil
		}

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

	it, err := idx.tx.Cursor(!reverse)
	if err != nil {
		return err
	}
	defer it.Close()

	prefix := idx.getKeyPrefix()

	seekPrefix := prefix
	if reverse {
		seekPrefix = append(seekPrefix, 255)
	}

	it.Seek(seekPrefix)

	for ; it.Valid(); it.Next() {
		item, err := it.Item()
		if err != nil {
			return err
		}

		key := item.Key
		if !bytes.HasPrefix(key, prefix) {
			return nil
		}

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
