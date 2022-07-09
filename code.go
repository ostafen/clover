package clover

import (
	"time"

	"github.com/google/orderedcode"
	"github.com/ostafen/clover/encoding"
	"github.com/ostafen/clover/util"
)

func getEncodeValue(value interface{}) interface{} {
	if util.IsNumber(value) {
		return util.ToFloat64(value) // each number is encoded as float64
	}

	switch vType := value.(type) {
	case bool:
		return uint64(util.BoolToInt(vType))
	case time.Time:
		return uint64(vType.UnixNano())
	}
	return value
}

func encodePrimitive(buf []byte, value interface{}, includeType bool) ([]byte, error) {
	var err error

	actualVal := getEncodeValue(value)
	if includeType {
		typeId := uint64(encoding.TypeId(value))
		buf, err = orderedcode.Append(buf, typeId)
		if err != nil {
			return nil, err
		}
	}

	if value == nil {
		return buf, nil
	}

	buf, err = orderedcode.Append(buf, actualVal)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func encode(buf []byte, v interface{}) ([]byte, error) {
	return encodeWithType(buf, v, false)
}

func encodeWithType(buf []byte, v interface{}, includeType bool) ([]byte, error) {
	switch vType := v.(type) {
	case map[string]interface{}:
		return encodeObject(buf, vType)
	case []interface{}:
		return encodeSlice(buf, vType)
	}
	return encodePrimitive(buf, v, includeType)
}

func encodeSlice(buf []byte, s []interface{}) ([]byte, error) {
	for _, v := range s {
		var err error
		buf, err = encodeWithType(buf, v, true)
		if err != nil {
			return nil, err
		}
	}
	return orderedcode.Append(make([]byte, 0), string(buf))
}

func encodeObject(buf []byte, o map[string]interface{}) ([]byte, error) {
	for _, key := range util.MapKeys(o, true) {
		value := o[key]

		encoded, err := orderedcode.Append(buf, key)
		if err != nil {
			return nil, err
		}

		buf, err = encodeWithType(encoded, value, true)
		if err != nil {
			return nil, err
		}
	}
	return orderedcode.Append(make([]byte, 0), string(buf))
}
