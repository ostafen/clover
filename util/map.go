package util

import "sort"

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

func MapKeys(m map[string]interface{}, sorted bool) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}

	if sorted {
		sort.Slice(keys, func(i, j int) bool {
			return keys[i] < keys[j]
		})
	}

	return keys
}
