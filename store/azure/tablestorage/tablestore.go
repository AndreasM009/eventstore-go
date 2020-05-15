package tablestorage

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/AndreasM009/eventstore-go/store"
	"github.com/Azure/azure-sdk-for-go/storage"
)

const (
	entityTableName        = "eventstoreentities"
	entityVersionTableName = "eventstoreversion"
	storageAccountName     = "storageAccountName"
	storageAccountKey      = "storageAccountKey"
	tableNameSuffix        = "tableNameSuffix"
)

type (
	tablestore struct {
		storageAccount         string
		storageAccountKey      string
		client                 storage.Client
		entityTableName        string
		entityVersionTableName string
		tableNameSuffix        string
	}
)

// NewStore creates a new Azure Table Storage based event store
func NewStore() store.EventStore {
	return &tablestore{}
}

func (s *tablestore) Init(metadata store.Metadata) error {
	var sa string
	var sk string

	sa, ok := metadata.Properties[storageAccountName]
	if !ok || sa == "" {
		return errors.New("azure tablestorage: storage account name is missing")
	}

	s.storageAccount = sa

	sk, ok = metadata.Properties[storageAccountKey]
	if !ok || sk == "" {
		return errors.New("azure tablestorage: storage account key is missing")
	}

	s.storageAccountKey = sk

	if sfx, ok := metadata.Properties[tableNameSuffix]; ok && sfx != "" {
		s.tableNameSuffix = sfx
	}

	s.entityTableName = fmt.Sprintf("%s%s", entityTableName, s.tableNameSuffix)
	s.entityVersionTableName = fmt.Sprintf("%s%s", entityVersionTableName, s.tableNameSuffix)

	client, err := storage.NewBasicClient(s.storageAccount, s.storageAccountKey)
	if err != nil {
		return err
	}

	s.client = client

	tbls := client.GetTableService()

	etbl := tbls.GetTableReference(s.entityTableName)

	if err := etbl.Get(10, storage.FullMetadata); err != nil {
		if err := etbl.Create(10, storage.EmptyPayload, nil); err != nil {
			return err
		}
	}

	vtbl := tbls.GetTableReference(s.entityVersionTableName)

	if err := vtbl.Get(10, storage.FullMetadata); err != nil {
		if err := vtbl.Create(10, storage.EmptyPayload, nil); err != nil {
			return err
		}
	}

	return nil
}

func (s *tablestore) Add(entity *store.Entity) (*store.Entity, error) {
	entity.Version = 1

	vtbl := s.getVersionTable()
	vety := s.makeVersionTableEntity(vtbl, entity)

	etbl := s.getEntityTable()
	eety, err := s.makeEntityTableEntity(etbl, entity)

	if err != nil {
		return nil, err
	}

	if err := vety.Insert(storage.EmptyPayload, nil); err != nil {
		return nil, store.EventStoreError{
			Text:       "insert version entity failed",
			ErrorType:  store.InternalError,
			InnerError: err,
		}
	}

	if err := eety.Insert(storage.EmptyPayload, nil); err != nil {
		return nil, store.EventStoreError{
			Text:       "insert entity failed",
			ErrorType:  store.InternalError,
			InnerError: err,
		}
	}

	return entity, nil
}

func (s *tablestore) Append(entity *store.Entity, concurrency store.ConcurrencyControl) (*store.Entity, error) {

	version, err := s.getNextVersionNumber(entity, concurrency)
	if err != nil {
		return nil, err
	}

	entity.Version = version
	etbl := s.getEntityTable()
	eety, err := s.makeEntityTableEntity(etbl, entity)

	if err != nil {
		return nil, err
	}

	if err := eety.Insert(storage.EmptyPayload, nil); err != nil {
		// so, here we have the problem, that the version is already incremented, but
		// the update of the entity failed!! Todo :-)
		return nil, store.EventStoreError{
			Text:       "failed to append new entity version",
			ErrorType:  store.InternalError,
			InnerError: err,
		}
	}

	return entity, nil
}

func (s *tablestore) getNextVersionNumber(entity *store.Entity, concurrency store.ConcurrencyControl) (int64, error) {
	for {
		vtbl := s.getVersionTable()
		vety := vtbl.GetEntityReference(entity.ID, entity.ID)

		// load version of entity, increment it and try to save it.
		// load full metadata, to check etag in merge
		if err := vety.Get(10, storage.FullMetadata, nil); err != nil {
			return 0, store.EventStoreError{
				Text:       "faild to load version entity",
				ErrorType:  store.EntityNotFound,
				InnerError: err,
			}
		}

		version, ok := vety.Properties["version"].(int64)
		if !ok {
			return 0, store.EventStoreError{
				Text:       "invalid type assertion for type version",
				ErrorType:  store.InternalError,
				InnerError: nil,
			}
		}

		// Optimistic Concurrency Control enabled?
		// check if we have the current verion of the entity or not
		if concurrency == store.Optimistic && entity.Version != version {
			// there is a newer version already stored, as we do OOL (Optimistic Offline Lock)
			// we return an error here
			return 0, store.EventStoreError{
				Text:       "entity has gone stale, a newer version already exists",
				ErrorType:  store.VersionConflict,
				InnerError: nil,
			}
		}

		version++
		vety.Properties["version"] = version

		if err := vety.Update(false, nil); err == nil {
			return version, nil
		}

		// try it again
	}
}

func (s *tablestore) GetLatestVersionNumber(id string) (int64, error) {
	vtbl := s.getVersionTable()
	vety := vtbl.GetEntityReference(id, id)

	if err := vety.Get(10, storage.FullMetadata, nil); err != nil {
		return int64(0), store.EventStoreError{
			Text:       "failed to load version of entity",
			ErrorType:  store.EntityNotFound,
			InnerError: err,
		}
	}

	version := vety.Properties["version"].(int64)
	return version, nil
}

func (s *tablestore) GetByVersion(id string, version int64) (*store.Entity, error) {
	tbl := s.getEntityTable()
	ety := tbl.GetEntityReference(id, fmt.Sprintf("%v", version))

	if err := ety.Get(10, storage.FullMetadata, nil); err != nil {
		return nil, store.EventStoreError{
			Text:       "failed to load version of entity",
			ErrorType:  store.EntityNotFound,
			InnerError: err,
		}
	}

	result := &store.Entity{}

	if err := json.Unmarshal(ety.Properties["data"].([]byte), result); err != nil {
		return nil, store.EventStoreError{
			Text:       "failed to deserialize entity",
			ErrorType:  store.SerializationFailed,
			InnerError: err,
		}
	}

	return result, nil
}

func (s *tablestore) GetByVersionRange(id string, startVersion, endVersion int64) ([]store.Entity, error) {
	tbl := s.getEntityTable()
	opts := storage.QueryOptions{
		Filter: fmt.Sprintf("(PartitionKey eq '%s') and (version ge %v) and (version le %v)", id, startVersion, endVersion),
	}

	result, err := tbl.QueryEntities(10, storage.FullMetadata, &opts)
	if err != nil {
		return nil, err
	}

	if len(result.Entities) == 0 {
		return []store.Entity{}, nil
	}

	resultEntities := make([]store.Entity, len(result.Entities))

	for i, e := range result.Entities {
		if err := json.Unmarshal(e.Properties["data"].([]byte), &resultEntities[i]); err != nil {
			return nil, store.EventStoreError{
				Text:       "failed to deserialize entity",
				ErrorType:  store.SerializationFailed,
				InnerError: err,
			}
		}
	}

	return resultEntities, nil
}

func (s *tablestore) makeVersionTableEntity(table *storage.Table, entity *store.Entity) *storage.Entity {
	props := map[string]interface{}{
		"version": entity.Version,
	}

	e := table.GetEntityReference(entity.ID, entity.ID)
	e.Properties = props
	return e
}

func (s *tablestore) makeEntityTableEntity(table *storage.Table, entity *store.Entity) (*storage.Entity, error) {
	data, err := json.Marshal(entity)
	if err != nil {
		return nil, store.EventStoreError{
			Text:       "faild to serialize entity",
			ErrorType:  store.SerializationFailed,
			InnerError: err,
		}
	}
	props := map[string]interface{}{
		"data":     data,
		"version":  entity.Version,
		"metadata": entity.Metadata,
	}

	e := table.GetEntityReference(entity.ID, fmt.Sprintf("%v", entity.Version))
	e.Properties = props
	return e, nil
}

func (s *tablestore) getVersionTable() *storage.Table {
	svc := s.client.GetTableService()
	return svc.GetTableReference(s.entityVersionTableName)
}

func (s *tablestore) getEntityTable() *storage.Table {
	svc := s.client.GetTableService()
	return svc.GetTableReference(s.entityTableName)
}
