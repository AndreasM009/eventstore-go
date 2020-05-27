package cosmosdb

import (
	"flag"
	"testing"

	"github.com/AndreasM009/eventstore-go/store"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

var (
	urlFlag       *string
	masterKeyFlag *string
	databaseFlag  *string
	containerFlag *string
)

func init() {
	urlFlag = flag.String("url", "", "CosmosDB URI")
	masterKeyFlag = flag.String("masterKey", "", "Cosmos account MasterKey")
	databaseFlag = flag.String("database", "", "Cosmos DB database name")
	containerFlag = flag.String("container", "", "CosmosDb container name")
}

func initMetadata() store.Metadata {
	metadata := store.Metadata{
		Properties: map[string]string{
			"Url":       *urlFlag,
			"MasterKey": *masterKeyFlag,
			"Database":  *databaseFlag,
			"Container": *containerFlag,
		},
	}
	return metadata
}

func TestInit(t *testing.T) {
	metadata := initMetadata()
	cosmos := NewStore()

	err := cosmos.Init(metadata)
	assert.Nil(t, err)
}

func TestAdd(t *testing.T) {
	metadata := initMetadata()
	cosmos := NewStore()

	err := cosmos.Init(metadata)
	assert.Nil(t, err)

	entity := store.Entity{
		ID:       uuid.New().String(),
		Version:  0,
		Metadata: "AddedEvent",
		Data:     "Hello World",
	}

	e, err := cosmos.Add(&entity)

	assert.Nil(t, err)
	assert.NotNil(t, entity)
	assert.Equal(t, int64(1), e.Version)
}

func TestAppend(t *testing.T) {
	metadata := initMetadata()
	cosmos := NewStore()

	err := cosmos.Init(metadata)
	assert.Nil(t, err)

	entity := store.Entity{
		ID:       uuid.New().String(),
		Version:  0,
		Metadata: "AddedEvent",
		Data:     "Hello World",
	}

	e, err := cosmos.Add(&entity)

	assert.Nil(t, err)
	assert.NotNil(t, entity)
	assert.Equal(t, int64(1), e.Version)

	entity.Data = "hello"

	e, err = cosmos.Append(&entity, store.None)

	assert.Nil(t, err)
	assert.NotNil(t, e)
	assert.Equal(t, int64(2), e.Version)
}

func TestAppendOptimisticConcurrencyControl(t *testing.T) {
	metadata := initMetadata()
	cosmos := NewStore()

	err := cosmos.Init(metadata)
	assert.Nil(t, err)

	entity := store.Entity{
		ID:       uuid.New().String(),
		Version:  0,
		Metadata: "AddedEvent",
		Data:     "Hello World",
	}

	e, err := cosmos.Add(&entity)

	assert.Nil(t, err)
	assert.NotNil(t, entity)
	assert.Equal(t, int64(1), e.Version)

	entity.Data = "hello"
	// try to add with old version
	entity.Version = 0

	e, err = cosmos.Append(&entity, store.Optimistic)

	assert.NotNil(t, err)
	assert.Nil(t, e)

	evterr, ok := err.(store.EventStoreError)
	assert.True(t, ok)
	assert.NotNil(t, evterr)
	assert.Equal(t, store.VersionConflict, evterr.ErrorType)
}

func TestGetLastestVersionNumber(t *testing.T) {
	metadata := initMetadata()
	cosmos := NewStore()

	err := cosmos.Init(metadata)
	assert.Nil(t, err)

	entity := store.Entity{
		ID:       uuid.New().String(),
		Version:  0,
		Metadata: "AddedEvent",
		Data:     "Hello World",
	}

	e, err := cosmos.Add(&entity)

	assert.Nil(t, err)
	assert.NotNil(t, entity)
	assert.Equal(t, int64(1), e.Version)

	version, err := cosmos.GetLatestVersionNumber(entity.ID)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), version)
}

func TestGetByVersion(t *testing.T) {
	metadata := initMetadata()
	cosmos := NewStore()

	err := cosmos.Init(metadata)
	assert.Nil(t, err)

	entity := store.Entity{
		ID:       uuid.New().String(),
		Version:  0,
		Metadata: "AddedEvent",
		Data:     "Hello World",
	}

	e, err := cosmos.Add(&entity)

	assert.Nil(t, err)
	assert.NotNil(t, entity)
	assert.Equal(t, int64(1), e.Version)

	entity.Data = "Hello"

	e, err = cosmos.Append(&entity, store.None)

	assert.Nil(t, err)
	assert.NotNil(t, e)
	assert.Equal(t, int64(2), e.Version)

	version, err := cosmos.GetLatestVersionNumber(entity.ID)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), version)

	e, err = cosmos.GetByVersion(entity.ID, int64(2))

	assert.Nil(t, err)
	assert.NotNil(t, e)
	assert.Equal(t, int64(2), e.Version)
	assert.Equal(t, "Hello", e.Data)
}

func TestGetByVersionRange(t *testing.T) {
	metadata := initMetadata()
	cosmos := NewStore()

	err := cosmos.Init(metadata)
	assert.Nil(t, err)

	entity := store.Entity{
		ID:       uuid.New().String(),
		Version:  0,
		Metadata: "AddedEvent",
		Data:     "Hello World",
	}

	e, err := cosmos.Add(&entity)

	assert.Nil(t, err)
	assert.NotNil(t, entity)
	assert.Equal(t, int64(1), e.Version)

	entity.Data = "Hello"

	e, err = cosmos.Append(&entity, store.None)

	assert.Nil(t, err)
	assert.NotNil(t, e)
	assert.Equal(t, int64(2), e.Version)

	entity.Data = "Hello GO"

	e, err = cosmos.Append(&entity, store.None)

	assert.Nil(t, err)
	assert.NotNil(t, e)
	assert.Equal(t, int64(3), e.Version)

	version, err := cosmos.GetLatestVersionNumber(entity.ID)
	assert.Nil(t, err)
	assert.Equal(t, int64(3), version)

	result, err := cosmos.GetByVersionRange(entity.ID, 1, 3)

	assert.Nil(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 3, len(result))
}
