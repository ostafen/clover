package clover

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

func listDir(dir string) ([]string, error) {
	fInfos, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	filenames := make([]string, 0, len(fInfos))
	for _, info := range fInfos {
		filenames = append(filenames, info.Name())
	}
	return filenames, nil
}

const defaultPermDir = 0777

func makeDirIfNotExists(dir string) error {
	if err := os.Mkdir(dir, defaultPermDir); err != nil && !os.IsExist(err) {
		return err
	}
	return nil
}

func getBasename(filename string) string {
	baseName := filepath.Base(filename)
	return strings.TrimSuffix(baseName, filepath.Ext(baseName))
}

func saveToFile(path string, filename string, data []byte) error {
	file, err := ioutil.TempFile("", filename)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := file.Write(data); err != nil {
		return err
	}

	return os.Rename(file.Name(), filepath.Join(path, filename))
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
