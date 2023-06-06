package store

type Store interface {
	Begin(update bool) (Tx, error)
	BeginWithUpdateBatch() (UpdateTx, error)
	Close() error
}

// updateTx only supports update and delete operations
type UpdateTx interface {
	Set(key, value []byte) error
	Delete(key []byte) error
	Commit() error
	Rollback() error
}

type Tx interface {
	UpdateTx
	Get(key []byte) ([]byte, error)
	Cursor(forward bool) (Cursor, error)
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
