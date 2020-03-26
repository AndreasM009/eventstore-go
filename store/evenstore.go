package store

// EventStore is the interface for an event store
type EventStore interface {
	Init(metadata Metadata) error
	Add(entity *Entity) (*Entity, error)
	Append(entity *Entity) (*Entity, error)

	GetLatestVersionNumber(id string) (int64, error)
	GetByVersion(id string, version int64) (*Entity, error)
}
