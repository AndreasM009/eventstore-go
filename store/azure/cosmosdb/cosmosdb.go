package cosmosdb

import (
	"encoding/json"
	"fmt"

	"github.com/AndreasM009/eventstore-impl/store"
	"github.com/a8m/documentdb"
)

type cosmosconnectioninfo struct {
	URL       string `json:"url"`
	MasterKey string `json:"masterKey"`
	Database  string `json:"database"`
	Container string `json:"container"`
}

type cosmosdb struct {
	connectionInfo cosmosconnectioninfo
	database       *documentdb.Database
	container      *documentdb.Collection
	client         *documentdb.DocumentDB
}

type cosmosentity struct {
	documentdb.Document
	ID       string        `json:"id"`
	EntityID string        `json:"entityId"`
	Version  int64         `json:"version"`
	Metadata string        `json:"metadata"`
	Type     string        `json:"type"`
	Data     *store.Entity `json:"data"`
}

type cosmosdbentityversion struct {
	documentdb.Document
	ID       string `json:"id"`
	EntityID string `json:"entityId"`
	Version  int64  `json:"version"`
	Type     string `json:"type"`
}

// NewStore create a new comsosdb store
func NewStore() store.EventStore {
	return &cosmosdb{}
}

func (c *cosmosdb) Init(metadata store.Metadata) error {
	s, err := json.Marshal(metadata.Properties)
	if err != nil {
		return err
	}

	var info cosmosconnectioninfo
	err = json.Unmarshal(s, &info)
	if err != nil {
		return err
	}
	c.connectionInfo = info

	client := documentdb.New(info.URL, &documentdb.Config{
		MasterKey: &documentdb.Key{
			Key: info.MasterKey,
		},
	})

	dbs, err := client.QueryDatabases(&documentdb.Query{
		Query: "SELECT * FROM ROOT r WHERE r.id=@id",
		Parameters: []documentdb.Parameter{
			{Name: "@id", Value: info.Database},
		},
	})

	if err != nil {
		return err
	} else if len(dbs) == 0 {
		return fmt.Errorf("Database %s for CosmosDB eventstore does not exit or was not found", info.Database)
	}

	c.database = &dbs[0]
	cntrs, err := client.QueryCollections(c.database.Self, &documentdb.Query{
		Query: "SELECT * FROM ROOT r WHERE r.id = @id",
		Parameters: []documentdb.Parameter{
			{Name: "@id", Value: info.Container},
		},
	})

	if err != nil {
		return err
	} else if len(cntrs) == 0 {
		return fmt.Errorf("Container %s in Database %s for CosmosDB eventstore not found", info.Container, info.Database)
	}

	c.container = &cntrs[0]
	c.client = client
	return nil
}

func (c *cosmosdb) Add(entity *store.Entity) (*store.Entity, error) {
	entity.Version = 1

	cosmosVersion := cosmosdbentityversion{
		ID:       entity.ID,
		EntityID: entity.ID,
		Version:  1,
		Type:     "version",
	}

	cosmosentity := cosmosentity{
		ID:       makeEntityVersion(entity.ID, 1),
		EntityID: entity.ID,
		Version:  1,
		Metadata: entity.Metadata,
		Data:     entity,
		Type:     "entity",
	}

	rqoptions := []documentdb.CallOption{
		documentdb.PartitionKey(entity.ID),
	}

	_, err := c.client.CreateDocument(c.container.Self, cosmosVersion, rqoptions...)

	if err != nil {
		return nil, store.EventStoreError{
			Text:       "insert version entity failed",
			ErrorType:  store.InternalError,
			InnerError: err,
		}
	}

	_, err = c.client.CreateDocument(c.container.Self, cosmosentity, rqoptions...)
	if err != nil {
		return nil, store.EventStoreError{
			Text:       "insert entity failed",
			ErrorType:  store.InternalError,
			InnerError: err,
		}
	}

	return entity, nil
}

func (c *cosmosdb) Append(entity *store.Entity, concurrency store.ConcurrencyControl) (*store.Entity, error) {
	version, err := c.getNextVersionNumber(entity, concurrency)

	if err != nil {
		return nil, err
	}

	entity.Version = version

	cosmosentity := cosmosentity{
		ID:       makeEntityVersion(entity.ID, version),
		EntityID: entity.ID,
		Version:  version,
		Metadata: entity.Metadata,
		Data:     entity,
		Type:     "entity",
	}

	options := []documentdb.CallOption{
		documentdb.PartitionKey(entity.ID),
	}

	_, err = c.client.CreateDocument(c.container.Self, cosmosentity, options...)

	if err != nil {
		return nil, store.EventStoreError{
			Text:       "failed to append new entity version",
			ErrorType:  store.InternalError,
			InnerError: err,
		}
	}

	return entity, nil
}

func (c *cosmosdb) GetLatestVersionNumber(id string) (int64, error) {
	options := []documentdb.CallOption{
		documentdb.PartitionKey(id),
	}

	cosmosVersions := []cosmosdbentityversion{}
	_, err := c.client.QueryDocuments(c.container.Self, &documentdb.Query{
		Query: "SELECT r.version FROM ROOT r WHERE r.id=@id and r.type=@type",
		Parameters: []documentdb.Parameter{
			{Name: "@id", Value: id},
			{Name: "@type", Value: "version"},
		},
	}, &cosmosVersions, options...)

	if err != nil || len(cosmosVersions) == 0 {
		return int64(0), store.EventStoreError{
			Text:       "failed to load version of entity",
			ErrorType:  store.EntityNotFound,
			InnerError: err,
		}
	}

	return cosmosVersions[0].Version, nil
}

func (c *cosmosdb) GetByVersion(id string, version int64) (*store.Entity, error) {
	options := []documentdb.CallOption{
		documentdb.PartitionKey(id),
	}

	cosmosEntities := []cosmosentity{}
	_, err := c.client.QueryDocuments(c.container.Self, &documentdb.Query{
		Query: "SELECT * FROM ROOT r WHERE r.id=@id and r.type=@type",
		Parameters: []documentdb.Parameter{
			{Name: "@id", Value: makeEntityVersion(id, version)},
			{Name: "@type", Value: "entity"},
		},
	}, &cosmosEntities, options...)

	if err != nil || len(cosmosEntities) == 0 {
		return nil, store.EventStoreError{
			Text:       "failed to load entity",
			ErrorType:  store.EntityNotFound,
			InnerError: err,
		}
	}

	return cosmosEntities[0].Data, nil
}

func (c *cosmosdb) GetByVersionRange(id string, startVersion int64, endVersion int64) ([]store.Entity, error) {
	options := []documentdb.CallOption{
		documentdb.PartitionKey(id),
	}

	cosmosEntities := []cosmosentity{}
	_, err := c.client.QueryDocuments(c.container.Self, &documentdb.Query{
		Query: fmt.Sprintf("SELECT * FROM ROOT r WHERE r.entityId=@entityId and r.type=@type and r.version >= %d and r.version <= %d", startVersion, endVersion),
		Parameters: []documentdb.Parameter{
			{Name: "@entityId", Value: id},
			{Name: "@type", Value: "entity"},
		},
	}, &cosmosEntities, options...)

	if err != nil || len(cosmosEntities) == 0 {
		return nil, store.EventStoreError{
			Text:       "failed to load entity versions",
			ErrorType:  store.EntityNotFound,
			InnerError: err,
		}
	}

	result := make([]store.Entity, len(cosmosEntities))

	for i, e := range cosmosEntities {
		result[i] = *e.Data
	}
	return result, nil
}

func (c *cosmosdb) getNextVersionNumber(entity *store.Entity, concurrency store.ConcurrencyControl) (int64, error) {
	rqoptions := []documentdb.CallOption{
		documentdb.PartitionKey(entity.ID),
	}

	for {
		cosmosVersions := []cosmosdbentityversion{}

		_, err := c.client.QueryDocuments(c.container.Self, &documentdb.Query{
			Query: "SELECT * FROM ROOT r WHERE r.id=@id and r.type=@type",
			Parameters: []documentdb.Parameter{
				{Name: "@id", Value: entity.ID},
				{Name: "@type", Value: "version"},
			},
		}, &cosmosVersions, rqoptions...)

		if err != nil {
			return 0, store.EventStoreError{
				Text:       fmt.Sprintf("CosmosDB eventstore: Version for %s not found", entity.ID),
				ErrorType:  store.EntityNotFound,
				InnerError: err,
			}
		} else if len(cosmosVersions) == 0 {
			return 0, store.EventStoreError{
				Text:       fmt.Sprintf("CosmosDB eventstore: Version for %s not found", entity.ID),
				ErrorType:  store.EntityNotFound,
				InnerError: err,
			}
		}

		cosmosVersion := &cosmosVersions[0]

		// check current version
		if concurrency == store.Optimistic && entity.Version != cosmosVersion.Version {
			return 0, store.EventStoreError{
				Text:       "entity has gone stale, a newer version already exists",
				ErrorType:  store.VersionConflict,
				InnerError: nil,
			}
		}

		cosmosVersion.Version++

		options := append(rqoptions, documentdb.IfMatch(cosmosVersion.Etag))

		_, err = c.client.UpsertDocument(c.container.Self, cosmosVersion, options...)

		if err != nil {
			rqerror := err.(documentdb.RequestError)

			if rqerror.Code == "412" {
				if concurrency == store.Optimistic {
					return 0, store.EventStoreError{
						Text:       "entity has gone stale, a newer version already exists",
						ErrorType:  store.VersionConflict,
						InnerError: nil,
					}
				}
			}
		} else {
			return cosmosVersion.Version, nil
		}
	}
}

func makeEntityVersion(id string, version int64) string {
	return fmt.Sprintf("%s--%d", id, version)
}
