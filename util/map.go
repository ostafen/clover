package util

func CopyMap(m map[string]interface{}) map[string]interface{} {
	mapCopy := make(map[string]interface{})
	for k, v := range m {
		mapValue, ok := v.(map[string]interface{})
		if ok {
			mapCopy[k] = CopyMap(mapValue)
		} else {
			mapCopy[k] = v
		}
	}
	return mapCopy
}
