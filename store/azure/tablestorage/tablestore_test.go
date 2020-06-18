package tablestorage

import (
	"flag"
	"testing"

	"github.com/AndreasM009/eventstore-go/store"

	"github.com/stretchr/testify/assert"
)

var (
	storageAccountFlag    *string
	storageAccountKeyFlag *string
	testMetadata          store.Metadata
	emptyTestMetadata     store.Metadata
)

func init() {
	storageAccountFlag = flag.String("storageaccount", "", "name of storage account to use")
	storageAccountKeyFlag = flag.String("storageaccountkey", "", "key of storage account to use")
	emptyTestMetadata.Properties = map[string]string{}
}

func initMetadata(suffix string) {
	testMetadata.Properties = map[string]string{
		storageAccountName: *storageAccountFlag,
		storageAccountKey:  *storageAccountKeyFlag,
		tableNameSuffix:    suffix,
	}
}

func destroyTestData(t *testing.T, s *tablestore) {
	tsvc := s.client.GetTableService()
	etbl := tsvc.GetTableReference(s.entityTableName)
	err := etbl.Delete(30, nil)
	assert.Nil(t, err)
}

func TestInit(t *testing.T) {
	initMetadata("t1")
	t.Logf("name:%s", testMetadata.Properties[storageAccountName])
	// test empty constrings
	s := NewStore()
	err := s.Init(emptyTestMetadata)
	assert.NotNil(t, err)

	// test init with connectionstrings
	s = NewStore()
	err = s.Init(testMetadata)
	assert.Nil(t, err)
	destroyTestData(t, s.(*tablestore))
}

func TestAddEntity(t *testing.T) {
	initMetadata("t2")
	s := NewStore()
	err := s.Init(testMetadata)
	assert.Nil(t, err)

	defer destroyTestData(t, s.(*tablestore))

	ety := &store.Entity{
		ID:       "1234",
		Data:     "Hello Wolrd",
		Metadata: "Metadata",
	}

	ety2, err := s.Add(ety)

	assert.Nil(t, err)
	assert.NotNil(t, ety2)
	assert.Equal(t, int64(1), ety2.Version)

	e, err := s.Add(ety2)
	assert.NotNil(t, err)
	assert.Nil(t, e)
}

func TestAppend(t *testing.T) {
	initMetadata("t3")
	s := NewStore()
	err := s.Init(testMetadata)
	assert.Nil(t, err)

	defer destroyTestData(t, s.(*tablestore))

	ety := &store.Entity{
		ID:       "1234",
		Data:     "Hello Wolrd",
		Metadata: "Metadata",
	}

	ety2, err := s.Add(ety)

	assert.Nil(t, err)
	assert.NotNil(t, ety2)
	assert.Equal(t, int64(1), ety2.Version)

	ety2.Data = "Hello EventStore"

	ety3, err := s.Append(ety2, store.Optimistic)
	assert.Nil(t, err)
	assert.NotNil(t, ety3)
	assert.Equal(t, int64(2), ety3.Version)
}

func TestAppendOldVersion(t *testing.T) {
	initMetadata("t4")
	s := NewStore()
	err := s.Init(testMetadata)
	assert.Nil(t, err)

	defer destroyTestData(t, s.(*tablestore))

	ety := &store.Entity{
		ID:       "1234",
		Data:     "Hello Wolrd",
		Metadata: "Metadata",
	}

	ety, err = s.Add(ety)

	assert.Nil(t, err)
	assert.NotNil(t, ety)
	assert.Equal(t, int64(1), ety.Version)

	ety.Data = "Hello EventStore"

	ety, err = s.Append(ety, store.Optimistic)
	assert.Nil(t, err)
	assert.NotNil(t, ety)
	assert.Equal(t, int64(2), ety.Version)

	etyOld := &store.Entity{
		ID:       "1234",
		Data:     "Hello",
		Metadata: "Metadata",
		Version:  1,
	}

	ety, err = s.Append(etyOld, store.Optimistic)
	assert.NotNil(t, err)
	assert.Nil(t, ety)

	evterr, ok := err.(store.EventStoreError)
	assert.True(t, ok)
	assert.NotNil(t, evterr)
	assert.Equal(t, store.VersionConflict, evterr.ErrorType)
}

func TestGetLatestVersionNumber(t *testing.T) {
	initMetadata("t5")
	s := NewStore()
	err := s.Init(testMetadata)
	assert.Nil(t, err)

	defer destroyTestData(t, s.(*tablestore))

	ety := &store.Entity{
		ID:       "1234",
		Data:     "Hello Wolrd",
		Metadata: "Metadata",
	}

	ety, err = s.Add(ety)

	assert.Nil(t, err)
	assert.NotNil(t, ety)
	assert.Equal(t, int64(1), ety.Version)

	ety.Data = "Hello EventStore"

	ety, err = s.Append(ety, store.Optimistic)
	assert.Nil(t, err)
	assert.NotNil(t, ety)
	assert.Equal(t, int64(2), ety.Version)

	version, err := s.GetLatestVersionNumber("1234")
	assert.Nil(t, err)
	assert.Equal(t, int64(2), version)
}

func TestGetLatestVersionNumberMissingEntity(t *testing.T) {
	initMetadata("t6")
	s := NewStore()
	err := s.Init(testMetadata)
	assert.Nil(t, err)

	defer destroyTestData(t, s.(*tablestore))

	version, err := s.GetLatestVersionNumber("1234")
	assert.NotNil(t, err)
	assert.Equal(t, int64(0), version)
}

func TestGetByVersion(t *testing.T) {
	initMetadata("t7")
	s := NewStore()
	err := s.Init(testMetadata)
	assert.Nil(t, err)

	defer destroyTestData(t, s.(*tablestore))

	ety := &store.Entity{
		ID:       "1234",
		Data:     "Hello World",
		Metadata: "Metadata",
	}

	ety, err = s.Add(ety)

	assert.Nil(t, err)
	assert.NotNil(t, ety)
	assert.Equal(t, int64(1), ety.Version)

	res, err := s.GetByVersion("1234", 1)
	assert.Nil(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, "Hello World", res.Data.(string))

	ety.Data = "Hello EventStore"

	ety, err = s.Append(ety, store.Optimistic)
	assert.Nil(t, err)
	assert.NotNil(t, ety)
	assert.Equal(t, int64(2), ety.Version)

	res, err = s.GetByVersion("1234", 2)
	assert.Nil(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, "Hello EventStore", res.Data.(string))
}

func TestGetByVersionRange(t *testing.T) {
	initMetadata("t8")
	s := NewStore()
	err := s.Init(testMetadata)
	assert.Nil(t, err)

	defer destroyTestData(t, s.(*tablestore))

	ety := &store.Entity{
		ID:       "12345",
		Data:     "1",
		Metadata: "Metadata",
	}

	ety, err = s.Add(ety)
	assert.Nil(t, err)

	ety.Data = "2"
	ety, err = s.Append(ety, store.Optimistic)
	assert.Nil(t, err)

	ety.Data = "3"
	ety, err = s.Append(ety, store.Optimistic)
	assert.Nil(t, err)

	entities, err := s.GetByVersionRange(ety.ID, 1, 3)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(entities))
}

func TestConcurrencyNone(t *testing.T) {
	initMetadata("t9")
	s := NewStore()
	err := s.Init(testMetadata)
	assert.Nil(t, err)

	defer destroyTestData(t, s.(*tablestore))

	ety := &store.Entity{
		ID:       "1234",
		Data:     "Hello Wolrd",
		Metadata: "Metadata",
	}

	_, err = s.Add(ety)
	assert.Nil(t, err)

	ety = &store.Entity{
		ID:       "1234",
		Data:     "Hello new Version",
		Metadata: "Metadata",
	}

	ety, err = s.Append(ety, store.None)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), ety.Version)

	ety.Data = "Hello"
	ety.Version = 0

	ety, err = s.Append(ety, store.None)
	assert.Nil(t, err)
	assert.Equal(t, int64(3), ety.Version)
}
