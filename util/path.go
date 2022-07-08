package util

import "os"

const defaultPermDir = 0777

func MakeDirIfNotExists(dir string) error {
	if err := os.Mkdir(dir, defaultPermDir); err != nil && !os.IsExist(err) {
		return err
	}
	return nil
}
