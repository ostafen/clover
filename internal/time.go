package internal

import (
	"time"

	"github.com/ostafen/clover/v2/util"
	"github.com/vmihailenco/msgpack/v5"
)

func init() {
	msgpack.RegisterExt(1, (*LocalizedTime)(nil))
}

type LocalizedTime struct {
	time.Time
}

var _ msgpack.Marshaler = (*LocalizedTime)(nil)
var _ msgpack.Unmarshaler = (*LocalizedTime)(nil)

func (tm *LocalizedTime) MarshalMsgpack() ([]byte, error) {
	return tm.GobEncode()
}

func (tm *LocalizedTime) UnmarshalMsgpack(b []byte) error {
	return tm.GobDecode(b)
}

func replaceTimes(v interface{}) interface{} {
	if t, isTime := v.(time.Time); isTime {
		return &LocalizedTime{t}
	}

	m, isMap := v.(map[string]interface{})
	if isMap {
		copyMap := util.CopyMap(m)
		for k, v := range m {
			copyMap[k] = replaceTimes(v)
		}
		return copyMap
	}

	s, isSlice := v.([]interface{})
	if isSlice {
		copyInterface := make([]interface{}, len(s))
		for i, v := range s {
			copyInterface[i] = replaceTimes(v)
		}
		return copyInterface
	}
	return v
}

func removeLocalizedTimes(v interface{}) interface{} {
	if t, isLTime := v.(*LocalizedTime); isLTime {
		return t.Time
	}

	m, isMap := v.(map[string]interface{})
	if isMap {
		for k, v := range m {
			m[k] = removeLocalizedTimes(v)
		}
	}

	s, isSlice := v.([]interface{})
	if isSlice {
		for i, v := range s {
			s[i] = replaceTimes(v)
		}
	}
	return v
}
