package encoding

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"
)

func init() {
	gob.Register(map[string]interface{}{})
	gob.Register([]interface{}{})
	gob.Register(time.Time{})
}

func processStructTag(tagStr string) (string, bool) {
	tags := strings.Split(tagStr, ",")
	name := tags[0] // when tagStr is "", tags[0] will also be ""
	omitempty := len(tags) > 1 && tags[1] == "omitempty"
	return name, omitempty
}

func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Pointer:
		return v.IsNil()
	}
	return false
}

func normalizeStruct(structValue reflect.Value) (map[string]interface{}, error) {
	m := make(map[string]interface{})
	for i := 0; i < structValue.NumField(); i++ {
		fieldType := structValue.Type().Field(i)
		fieldValue := structValue.Field(i)

		if fieldType.IsExported() {
			fieldName := fieldType.Name

			cloverTag := fieldType.Tag.Get("clover")
			name, omitempty := processStructTag(cloverTag)
			if name != "" {
				fieldName = name
			}

			if !omitempty || !isEmptyValue(fieldValue) {
				normalized, err := Normalize(structValue.Field(i).Interface())
				if err != nil {
					return nil, err
				}
				m[fieldName] = normalized
			}
		}
	}

	return m, nil
}

func normalizeSlice(sliceValue reflect.Value) (interface{}, error) {
	if sliceValue.Type().Elem().Kind() == reflect.Uint8 {
		return sliceValue.Interface(), nil
	}

	s := make([]interface{}, 0)
	for i := 0; i < sliceValue.Len(); i++ {
		v, err := Normalize(sliceValue.Index(i).Interface())
		if err != nil {
			return nil, err
		}
		s = append(s, v)
	}
	return s, nil
}

func getElemValueAndType(v interface{}) (reflect.Value, reflect.Type) {
	rv := reflect.ValueOf(v)
	rt := reflect.TypeOf(v)

	for rt.Kind() == reflect.Ptr && !rv.IsNil() {
		rt = rt.Elem()
		rv = rv.Elem()
	}
	return rv, rt
}

func normalizeMap(mapValue reflect.Value) (map[string]interface{}, error) {
	if mapValue.Type().Key().Kind() != reflect.String {
		return nil, fmt.Errorf("map key type must be a string")
	}

	m := make(map[string]interface{})
	for _, key := range mapValue.MapKeys() {
		value := mapValue.MapIndex(key)

		normalized, err := Normalize(value.Interface())
		if err != nil {
			return nil, err
		}
		m[key.String()] = normalized
	}
	return m, nil
}

func Normalize(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	rValue, rType := getElemValueAndType(value)
	if rType.Kind() == reflect.Ptr {
		return nil, nil
	}

	if _, isTime := rValue.Interface().(time.Time); isTime {
		return rValue.Interface(), nil
	}

	switch rType.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return rValue.Uint(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rValue.Int(), nil
	case reflect.Float32, reflect.Float64:
		return rValue.Float(), nil
	case reflect.Struct:
		return normalizeStruct(rValue)
	case reflect.Map:
		return normalizeMap(rValue)
	case reflect.String:
		return rValue.String(), nil
	case reflect.Bool:
		return rValue.Bool(), nil
	case reflect.Slice:
		return normalizeSlice(rValue)
	}
	return nil, fmt.Errorf("invalid dtype %s", rType.Name())
}

func createRenameMap(rv reflect.Value) map[string]string {
	renameMap := make(map[string]string)
	for i := 0; i < rv.NumField(); i++ {
		fieldType := rv.Type().Field(i)

		tagStr, found := fieldType.Tag.Lookup("clover")
		if found {
			name, _ := processStructTag(tagStr)
			renameMap[name] = fieldType.Name
		}
	}
	return renameMap
}

func rename(fields map[string]interface{}, v interface{}) map[string]interface{} {
	rv := reflect.ValueOf(v)
	if rv.Type().Kind() != reflect.Struct {
		return nil
	}

	renameMap := createRenameMap(rv)
	m := make(map[string]interface{})
	for key, value := range fields {
		renamedFieldName := renameMap[key]
		if renamedFieldName != "" {
			m[renamedFieldName] = value
			delete(m, key)
		} else {
			m[key] = value
		}
	}
	return m
}

func getElemType(rt reflect.Type) reflect.Type {
	for rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}
	return rt
}

func renameMapKeys(m map[string]interface{}, v interface{}) map[string]interface{} {
	rv, rt := getElemValueAndType(v)
	if rt.Kind() != reflect.Struct {
		return m
	}

	renamed := rename(m, rv.Interface())
	for i := 0; i < rv.NumField(); i++ {
		sf := rv.Type().Field(i)
		fv := renamed[sf.Name]
		ft := getElemType(sf.Type)

		fMap, isMap := fv.(map[string]interface{})
		if isMap && ft.Kind() == reflect.Struct {
			converted := renameMapKeys(fMap, rv.Field(i).Interface())
			renamed[sf.Name] = converted
		}
	}
	return renamed
}

func Encode(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(v)
	return buf.Bytes(), err
}

func Decode(data []byte, v interface{}) error {
	buf := bytes.NewBuffer(data)
	return gob.NewDecoder(buf).Decode(v)
}

func Convert(m map[string]interface{}, v interface{}) error {
	renamed := renameMapKeys(m, v)

	b, err := json.Marshal(renamed)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}
