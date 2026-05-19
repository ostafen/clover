package index

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
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

type rangeIndex struct {
	indexBase
	tx store.Tx
}

func extractDocId(key []byte) ([]byte, []byte) {
	if len(key) < 36 {
		panic(string(key))
	}
	return key[:len(key)-36], key[len(key)-36:]
}

func (idx *rangeIndex) getKeyPrefix() []byte {
	return []byte(fmt.Sprintf("c:%s;i:%s", idx.collection, strings.Join(idx.fields, ",")))
}

func (idx *rangeIndex) getKey(value interface{}) ([]byte, error) {
	var values []interface{}
	if s, ok := value.([]interface{}); ok {
		values = s
	} else if value != nil {
		values = []interface{}{value}
	}
	return internal.OrderedCode(idx.getKeyPrefix(), true, values...)
}

func (idx *rangeIndex) encodeValueAndId(values []interface{}, docId string) ([]byte, error) {
	encodedKey, err := idx.getKey(values)
	if err != nil {
		return nil, err
	}
	encodedKey = append(encodedKey, []byte(docId)...)
	return encodedKey, nil
}

func (idx *rangeIndex) Add(docId string, values []interface{}, ttl time.Duration) error {
	encodedKey, err := idx.encodeValueAndId(values, docId)
	if err != nil {
		return err
	}
	return idx.tx.Set(encodedKey, nil)
}

func (idx *rangeIndex) Remove(docId string, values []interface{}) error {
	encodedKey, err := idx.encodeValueAndId(values, docId)
	if err != nil {
		return err
	}
	return idx.tx.Delete(encodedKey)
}

func (idx *rangeIndex) Drop() error {
	return idx.tx.DeletePrefix(idx.getKeyPrefix())
}

func (idx *rangeIndex) encodeRange(vRange *Range) ([]byte, []byte, error) {
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

func (idx *rangeIndex) IterateRange(vRange *Range, reverse bool, onValue func(docId string) error) error {
	if vRange.IsEmpty() {
		return nil
	}

	startKey, endKey, err := idx.encodeRange(vRange)
	if err != nil {
		return err
	}

	seekPrefix := startKey
	if reverse {
		if endKey != nil {
			seekPrefix = append(append([]byte(nil), endKey...), 255)
		} else {
			seekPrefix = append(idx.getKeyPrefix(), 255)
		}
	} else if seekPrefix == nil {
		seekPrefix = idx.getKeyPrefix()
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
				if bytes.HasPrefix(p, endKey) && vRange.EndIncluded {
					// Do not break; p is an extension of endKey and End is included.
				} else {
					break
				}
			}
		} else {
			startCmp := bytes.Compare(p, startKey)
			if (vRange.Start != nil || vRange.IsNil()) && (startCmp < 0 || (startCmp == 0 && !vRange.StartIncluded)) {
				if bytes.HasPrefix(p, startKey) && vRange.StartIncluded {
					// Do not break; p is an extension of startKey and Start is included.
				} else {
					break
				}
			}
		}

		if err := onValue(string(docId)); err != nil {
			if errors.Is(err, internal.ErrStopIteration) {
				return nil
			}
			return err
		}
	}
	return nil
}

func (idx *rangeIndex) Iterate(reverse bool, onValue func(docId string) error) error {
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
			if errors.Is(err, internal.ErrStopIteration) {
				return nil
			}
			return err
		}
	}
	return nil
}

func (idx *rangeIndex) Type() Type {
	if len(idx.fields) > 1 {
		return Compound
	}
	return SingleField
}
