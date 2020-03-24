package tablestorage

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/AndreasM009/eventstore-go/store"
	"github.com/Azure/azure-sdk-for-go/storage"
)

const (
	entityTableName        = "eventstoreentities"
	entityVersionTableName = "eventstoreversion"
)

type (
	tablestore struct {
		storageAccount         string
		storageAccountKey      string
		client                 storage.Client
		entityTableName        string
		entityVersionTableName string
	}
)

// NewStore creates a new Azure Table Storage based event store
func NewStore(storageAccount, storageAccountKey, tableSuffix string) store.EventStore {
	return &tablestore{
		storageAccount:         storageAccount,
		storageAccountKey:      storageAccountKey,
		entityTableName:        fmt.Sprintf("%s%s", entityTableName, tableSuffix),
		entityVersionTableName: fmt.Sprintf("%s%s", entityVersionTableName, tableSuffix),
	}
}

func (s *tablestore) Init() error {
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
	eety := s.makeEntityTableEntity(etbl, entity)

	if err := vety.Insert(storage.EmptyPayload, nil); err != nil {
		return nil, err
	}

	if err := eety.Insert(storage.EmptyPayload, nil); err != nil {
		return nil, err
	}

	return entity, nil
}

func (s *tablestore) Append(entity *store.Entity) (*store.Entity, error) {

	vtbl := s.getVersionTable()
	vety := vtbl.GetEntityReference(entity.ID, entity.ID)

	// load version of entity, increment it and try to save it.
	// load full metadata, to check etag in merge
	if err := vety.Get(10, storage.FullMetadata, nil); err != nil {
		return nil, err
	}

	version, ok := vety.Properties["version"].(int64)
	if !ok {
		return nil, errors.New("Invalid type assertion for type version")
	}

	// check if we have the current verion of the entity or not
	if entity.Version != version {
		// there is a newer version already stored, as we do OOL (Optimistic Offline Lock)
		// we return an error here
		return nil, errors.New("Entity gone stale, newer version already available (OOL)")
	}

	version++
	vety.Properties["version"] = version

	if err := vety.Update(false, nil); err != nil {
		// if we are here, the entity was updated form another user task, etc.
		// at the moment we just return an error as we do an OOL (Optimistic Offline Lock)
		return nil, err
	}

	entity.Version = version
	etbl := s.getEntityTable()
	eety := s.makeEntityTableEntity(etbl, entity)

	if err := eety.Insert(storage.EmptyPayload, nil); err != nil {
		// so, here we have the problem, that the version is already incremented, but
		// the update of the entity failed!! Todo :-)
		return nil, err
	}

	return entity, nil
}

func (s *tablestore) GetLatestVersionNumber(id string) (int64, error) {
	vtbl := s.getVersionTable()
	vety := vtbl.GetEntityReference(id, id)

	if err := vety.Get(10, storage.FullMetadata, nil); err != nil {
		return int64(0), err
	}

	version := vety.Properties["version"].(int64)
	return version, nil
}

func (s *tablestore) GetByVersion(id string, version int64) (*store.Entity, error) {
	tbl := s.getEntityTable()
	ety := tbl.GetEntityReference(id, fmt.Sprintf("%v", version))

	if err := ety.Get(10, storage.FullMetadata, nil); err != nil {
		return nil, err
	}

	version, err := strconv.ParseInt(ety.RowKey, 10, 64)
	if err != nil {
		return nil, err
	}

	return &store.Entity{
		ID:      id,
		Version: version,
		Data:    ety.Properties["data"].([]byte),
	}, nil
}

func (s *tablestore) makeVersionTableEntity(table *storage.Table, entity *store.Entity) *storage.Entity {
	props := map[string]interface{}{
		"version": entity.Version,
	}

	e := table.GetEntityReference(entity.ID, entity.ID)
	e.Properties = props
	return e
}

func (s *tablestore) makeEntityTableEntity(table *storage.Table, entity *store.Entity) *storage.Entity {
	props := map[string]interface{}{
		"data": entity.Data,
	}

	e := table.GetEntityReference(entity.ID, fmt.Sprintf("%v", entity.Version))
	e.Properties = props
	return e
}

func (s *tablestore) getVersionTable() *storage.Table {
	svc := s.client.GetTableService()
	return svc.GetTableReference(s.entityVersionTableName)
}

func (s *tablestore) getEntityTable() *storage.Table {
	svc := s.client.GetTableService()
	return svc.GetTableReference(s.entityTableName)
}
