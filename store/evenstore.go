package store

// ConcurrencyControl controls concurrency handlen when new entities are append to the event store
type ConcurrencyControl int

const (
	// None no cuncurrency control
	None ConcurrencyControl = iota
	// Optimistic OptimisticConcurrencyControl
	Optimistic ConcurrencyControl = iota
)

// EventStore is the interface for an event store
type EventStore interface {
	Init(metadata Metadata) error
	Add(entity *Entity) (*Entity, error)
	Append(entity *Entity, concurrency ConcurrencyControl) (*Entity, error)

	GetLatestVersionNumber(id string) (int64, error)
	GetByVersion(id string, version int64) (*Entity, error)
	GetByVersionRange(id string, startVersion int64, endVersion int64) ([]Entity, error)
}
