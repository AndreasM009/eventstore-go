package store

// Entity represents a record in the EventStore
type Entity struct {
	ID       string      `json:"id"`
	Version  int64       `json:"version"`
	Metadata string      `json:"metadata"`
	Data     interface{} `json:"data"`
}
