package store

// Entity represents a record in the EventStore
type Entity struct {
	ID      string
	Version int64
	Data    []byte
}
