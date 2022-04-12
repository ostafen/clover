package clover

import (
	"encoding/json"
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

func normalize(value interface{}) (interface{}, error) {
	var normalized interface{}
	bytes, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(bytes, &normalized)
	return normalized, err
}
