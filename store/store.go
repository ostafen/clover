package store

type Store interface {
	Begin(update bool) (Tx, error)
	Close() error
}

type Tx interface {
	Set(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	Cursor(forward bool) (Cursor, error)
	Commit() error
	Rollback() error
}

type Cursor interface {
	Seek(key []byte) error
	Next()
	Valid() bool
	Item() (Item, error)
	Close() error
}

type Item struct {
	Key, Value []byte
}
