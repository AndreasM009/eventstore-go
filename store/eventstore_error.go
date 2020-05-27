package store

import "fmt"

// ErrorType is the base type of error that are returned
type ErrorType int

const (
	// SerializationFailed is returned when JSON serialization fails
	SerializationFailed ErrorType = iota + 1
	// EntityNotFound is returned when a requested entity was not found
	EntityNotFound
	// VersionConflict is returned when an entity should be saved with a too old version
	VersionConflict
	// InternalError is returned i9n all other casses
	InternalError
)

// EventStoreError that is returned in case of an error
type EventStoreError struct {
	Text       string
	ErrorType  ErrorType
	InnerError error
}

func (e EventStoreError) Error() string {
	return fmt.Sprintf("%s -- Inner error: %s", e.Text, e.InnerError)
}
