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

func MapKeys(m map[string]interface{}, sorted bool, includeSubKeys bool) []string {
	keys := make([]string, 0, len(m))
	for key, value := range m {
		added := false
		if includeSubKeys {
			subMap, isMap := value.(map[string]interface{})
			if isMap {
				subFields := MapKeys(subMap, false, includeSubKeys)
				for _, subKey := range subFields {
					keys = append(keys, key+"."+subKey)
				}
				added = true
			}
		}

		if !added {
			keys = append(keys, key)
		}
	}

	if sorted {
		sort.Slice(keys, func(i, j int) bool {
			return keys[i] < keys[j]
		})
	}

	return keys
}

func StringSliceToSet(s []string) map[string]bool {
	set := make(map[string]bool)
	for _, str := range s {
		set[str] = true
	}
	return set
}
