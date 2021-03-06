package clover

import (
	"math/big"
	"reflect"
	"sort"
	"strings"
	"time"
)

var typesMap map[string]int = map[string]int{
	"nil":     0,
	"number":  1,
	"string":  2,
	"map":     3,
	"slice":   4,
	"boolean": 5,
	"time":    6,
}

func getTypeName(v interface{}) string {
	if isNumber(v) {
		return "number"
	}

	switch v.(type) {
	case nil:
		return "null"
	case time.Time:
		return "time"
	}

	return reflect.TypeOf(v).Kind().String()
}

func compareTypes(v1 interface{}, v2 interface{}) int {
	t1 := getTypeName(v1)
	t2 := getTypeName(v2)
	return typesMap[t1] - typesMap[t2]
}

func compareSlices(s1 []interface{}, s2 []interface{}) int {
	for i := 0; i < len(s1) && i < len(s2); i++ {
		if res := compareValues(s1[i], s2[i]); res != 0 {
			return res
		}
	}
	return len(s1) - len(s2)
}

func compareNumbers(v1 interface{}, v2 interface{}) int {
	_, isV1Float := v1.(float64)
	_, isV2Float := v2.(float64)

	if isV1Float || isV2Float {
		v1Float := toFloat64(v1)
		v2Float := toFloat64(v2)
		return big.NewFloat(v1Float).Cmp(big.NewFloat(v2Float))
	}

	_, isV1Int64 := v1.(int64)
	_, isV2Int64 := v2.(int64)

	if isV1Int64 || isV2Int64 {
		v1Int64 := toInt64(v1)
		v2Int64 := toInt64(v2)
		return int(v1Int64 - v2Int64)
	}

	v1Uint64 := v1.(uint64)
	v2Uint64 := v2.(uint64)
	return int(v1Uint64 - v2Uint64)
}

func compareValues(v1 interface{}, v2 interface{}) int {
	if res := compareTypes(v1, v2); res != 0 {
		return res
	}

	if isNumber(v1) && isNumber(v2) {
		return compareNumbers(v1, v2)
	}

	v1Str, isStr := v1.(string)
	if isStr {
		v2Str := v2.(string)
		return strings.Compare(v1Str, v2Str)
	}

	v1Bool, isBool := v1.(bool)
	if isBool {
		v2Bool := v2.(bool)
		return boolToInt(v1Bool) - boolToInt(v2Bool)
	}

	v1Time, isTime := v1.(time.Time)
	if isTime {
		v2Time := v2.(time.Time)
		return int(v1Time.UnixNano() - v2Time.UnixNano())
	}

	v1Slice, isSlice := v1.([]interface{})
	if isSlice {
		return compareSlices(v1Slice, v2.([]interface{}))
	}

	if v1 == nil {
		return 0
	}

	return compareObjects(v1.(map[string]interface{}), v2.(map[string]interface{}))
}

func getKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	return keys
}

func compareObjects(m1 map[string]interface{}, m2 map[string]interface{}) int {
	m1Keys := getKeys(m1)
	m2Keys := getKeys(m2)

	for i := 0; i < len(m1Keys); i++ {
		k1 := m1Keys[i]
		k2 := m2Keys[i]

		if res := strings.Compare(k1, k2); res != 0 {
			return res
		}

		v1 := m1[k1]
		v2 := m2[k2]

		if res := compareValues(v1, v2); res != 0 {
			return res
		}
	}
	return len(m1) - len(m2)
}
