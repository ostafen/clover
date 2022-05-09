package clover

import (
	"os"
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

func toInt64(v interface{}) int64 {
	switch vType := v.(type) {
	case uint64:
		return int64(vType)
	case int64:
		return vType
	}
	panic("not a number")
}
