package clover

import (
	"fmt"
	"os"
	"reflect"
	"time"
)

const defaultPermDir = 0777

func makeDirIfNotExists(dir string) error {
	if err := os.Mkdir(dir, defaultPermDir); err != nil && !os.IsExist(err) {
		return err
	}
	return nil
}

func copyMap(m map[string]interface{}) map[string]interface{} {
	mapCopy := make(map[string]interface{})
	for k, v := range m {
		mapValue, ok := v.(map[string]interface{})
		if ok {
			mapCopy[k] = copyMap(mapValue)
		} else {
			mapCopy[k] = v
		}
	}
	return mapCopy
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func convertMap(mapValue reflect.Value) (map[string]interface{}, error) {
	// check if type is map (this is intended to be used directly)

	if mapValue.Type().Key().Kind() != reflect.String {
		return nil, fmt.Errorf("map key type must be a string")
	}

	m := make(map[string]interface{})
	for _, key := range mapValue.MapKeys() {
		value := mapValue.MapIndex(key)

		normalized, err := normalize(value.Interface())
		if err != nil {
			return nil, err
		}
		m[key.String()] = normalized
	}
	return m, nil
}

func convertStruct(structValue reflect.Value) (map[string]interface{}, error) {
	m := make(map[string]interface{})
	for i := 0; i < structValue.NumField(); i++ {
		fieldName := structValue.Type().Field(i).Name
		fieldValue := structValue.Field(i)

		if fieldValue.CanInterface() {
			normalized, err := normalize(structValue.Field(i).Interface())
			if err != nil {
				return nil, err
			}
			m[fieldName] = normalized
		}
	}
	return m, nil
}

func convertSlice(sliceValue reflect.Value) ([]interface{}, error) {
	s := make([]interface{}, 01)
	for i := 0; i < sliceValue.Len(); i++ {
		v, err := normalize(sliceValue.Index(i).Interface())
		if err != nil {
			return nil, err
		}
		s = append(s, v)
	}
	return s, nil
}

func normalize(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	if _, isTime := value.(time.Time); isTime {
		return value, nil
	}

	rValue := reflect.ValueOf(value)
	rType := reflect.TypeOf(value)

	switch rType.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rValue.Convert(reflect.TypeOf(int64(0))).Interface(), nil
	case reflect.Float32, reflect.Float64:
		return rValue.Convert(reflect.TypeOf(float64(0))).Interface(), nil
	case reflect.Struct:
		return convertStruct(rValue)
	case reflect.Map:
		return convertMap(rValue)
	case reflect.String, reflect.Bool:
		return value, nil
	case reflect.Slice:
		return convertSlice(rValue)
	}

	return nil, fmt.Errorf("invalid dtype %s", rType.Name())
}

func isNumber(v interface{}) bool {
	switch v.(type) {
	case int, uint, uint8, uint16, uint32, uint64,
		int8, int16, int32, int64, float32, float64:
		return true
	default:
		return false
	}
}

func toFloat64(v interface{}) float64 {
	switch vType := v.(type) {
	case uint64:
		return float64(vType)
	case int64:
		return float64(vType)
	case float64:
		return vType
	}
	panic("not a number")
}

func toUint64(v interface{}) int64 {
	switch vType := v.(type) {
	case uint64:
		return int64(vType)
	case int64:
		return vType
	}
	panic("not a number")
}
