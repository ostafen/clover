package util

func IsNumber(v interface{}) bool {
	switch v.(type) {
	case int, uint, uint8, uint16, uint32, uint64,
		int8, int16, int32, int64, float32, float64:
		return true
	default:
		return false
	}
}

func ToFloat64(v interface{}) float64 {
	switch vType := v.(type) {
	case int:
		return float64(vType)
	case int8:
		return float64(vType)
	case int16:
		return float64(vType)
	case int32:
		return float64(vType)
	case int64:
		return float64(vType)
	case uint:
		return float64(vType)
	case uint8:
		return float64(vType)
	case uint16:
		return float64(vType)
	case uint32:
		return float64(vType)
	case uint64:
		return float64(vType)
	case float32:
		return float64(vType)
	case float64:
		return vType
	}
	panic("not a number")
}

func ToInt64(v interface{}) int64 {
	switch vType := v.(type) {
	case int:
		return int64(vType)
	case int8:
		return int64(vType)
	case int16:
		return int64(vType)
	case int32:
		return int64(vType)
	case int64:
		return int64(vType)
	case uint:
		return int64(vType)
	case uint8:
		return int64(vType)
	case uint16:
		return int64(vType)
	case uint32:
		return int64(vType)
	case uint64:
		return int64(vType)
	case float32:
		return int64(vType)
	case float64:
		return int64(vType)
	}
	panic("not a number")
}

func BoolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
