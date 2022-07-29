package internal

import (
	"math/big"
	"reflect"
	"strings"
	"time"

	"github.com/ostafen/clover/v2/util"
)

var typesMap map[string]int = map[string]int{
	"nil":    0,
	"number": 1,
	"string": 2,
	"map":    3,
	"slice":  4,
	"bool":   5,
	"time":   6,
}

func TypeName(v interface{}) string {
	if util.IsNumber(v) {
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

func TypeId(v interface{}) int {
	return typesMap[TypeName(v)]
}

func compareTypes(v1 interface{}, v2 interface{}) int {
	return TypeId(v1) - TypeId(v2)
}

func compareSlices(s1 []interface{}, s2 []interface{}) int {
	for i := 0; i < len(s1) && i < len(s2); i++ {
		if res := Compare(s1[i], s2[i]); res != 0 {
			return res
		}
	}
	return len(s1) - len(s2)
}

func compareNumbers(v1 interface{}, v2 interface{}) int {
	_, isV1Float := v1.(float64)
	_, isV2Float := v2.(float64)

	if isV1Float || isV2Float {
		v1Float := util.ToFloat64(v1)
		v2Float := util.ToFloat64(v2)
		return big.NewFloat(v1Float).Cmp(big.NewFloat(v2Float))
	}

	_, isV1Int64 := v1.(int64)
	_, isV2Int64 := v2.(int64)

	if isV1Int64 || isV2Int64 {
		v1Int64 := util.ToInt64(v1)
		v2Int64 := util.ToInt64(v2)
		return int(v1Int64 - v2Int64)
	}

	v1Uint64 := v1.(uint64)
	v2Uint64 := v2.(uint64)
	return int(v1Uint64 - v2Uint64)
}

func Compare(v1 interface{}, v2 interface{}) int {
	if res := compareTypes(v1, v2); res != 0 {
		return res
	}

	if util.IsNumber(v1) && util.IsNumber(v2) {
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
		return util.BoolToInt(v1Bool) - util.BoolToInt(v2Bool)
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

func compareObjects(m1 map[string]interface{}, m2 map[string]interface{}) int {
	m1Keys := util.MapKeys(m1, true, false)
	m2Keys := util.MapKeys(m2, true, false)

	for i := 0; i < len(m1Keys) && i < len(m2Keys); i++ {
		k1 := m1Keys[i]
		k2 := m2Keys[i]

		if res := strings.Compare(k1, k2); res != 0 {
			return res
		}

		v1 := m1[k1]
		v2 := m2[k2]

		if res := Compare(v1, v2); res != 0 {
			return res
		}
	}
	return len(m1) - len(m2)
}
