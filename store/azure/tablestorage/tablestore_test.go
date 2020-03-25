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
)

func init() {
	storageAccountFlag = flag.String("storageaccount", "", "name of storage account to use")
	storageAccountKeyFlag = flag.String("storageaccountkey", "", "key of storage account to use")
}

func destroyTestData(s *tablestore) {
	tsvc := s.client.GetTableService()
	vtbl := tsvc.GetTableReference(s.entityVersionTableName)
	vtbl.Delete(30, nil)
	etbl := tsvc.GetTableReference(s.entityTableName)
	etbl.Delete(30, nil)
}

func TestInit(t *testing.T) {
	storageAccounName := *storageAccountFlag
	storageAccountKey := *storageAccountKeyFlag

	// test empty constrings
	s := NewStore("", "", "")
	err := s.Init()
	assert.NotNil(t, err)

	// test init with connectionstrings
	s = NewStore(storageAccounName, storageAccountKey, "t1")
	err = s.Init()
	assert.Nil(t, err)
	destroyTestData(s.(*tablestore))
}

func TestAddEntity(t *testing.T) {
	s := NewStore(*storageAccountFlag, *storageAccountKeyFlag, "t2")
	err := s.Init()
	assert.Nil(t, err)

	defer destroyTestData(s.(*tablestore))

	ety := &store.Entity{
		ID:   "1234",
		Data: "Hello Wolrd",
	}

	ety2, err := s.Add(ety)

	assert.Nil(t, err)
	assert.NotNil(t, ety2)
	assert.Equal(t, int64(1), ety2.Version)

	_, err = s.Add(ety2)
	assert.NotNil(t, err)
}

func TestAppend(t *testing.T) {
	s := NewStore(*storageAccountFlag, *storageAccountKeyFlag, "t3")
	err := s.Init()
	assert.Nil(t, err)

	defer destroyTestData(s.(*tablestore))

	ety := &store.Entity{
		ID:   "1234",
		Data: "Hello Wolrd",
	}

	ety2, err := s.Add(ety)

	assert.Nil(t, err)
	assert.NotNil(t, ety2)
	assert.Equal(t, int64(1), ety2.Version)

	ety2.Data = "Hello EventStore"

	ety3, err := s.Append(ety2)
	assert.Nil(t, err)
	assert.NotNil(t, ety3)
	assert.Equal(t, int64(2), ety3.Version)
}

func TestAppendOldVersion(t *testing.T) {
	s := NewStore(*storageAccountFlag, *storageAccountKeyFlag, "t4")
	err := s.Init()
	assert.Nil(t, err)

	defer destroyTestData(s.(*tablestore))

	ety := &store.Entity{
		ID:   "1234",
		Data: "Hello Wolrd",
	}

	ety, err = s.Add(ety)

	assert.Nil(t, err)
	assert.NotNil(t, ety)
	assert.Equal(t, int64(1), ety.Version)

	ety.Data = "Hello EventStore"

	ety, err = s.Append(ety)
	assert.Nil(t, err)
	assert.NotNil(t, ety)
	assert.Equal(t, int64(2), ety.Version)

	etyOld := &store.Entity{
		ID:      "1234",
		Data:    "Hello",
		Version: 1,
	}

	ety, err = s.Append(etyOld)
	assert.NotNil(t, err)
	assert.Nil(t, ety)
}

func TestGetLatestVersionNumber(t *testing.T) {
	s := NewStore(*storageAccountFlag, *storageAccountKeyFlag, "t5")
	err := s.Init()
	assert.Nil(t, err)

	defer destroyTestData(s.(*tablestore))

	ety := &store.Entity{
		ID:   "1234",
		Data: "Hello Wolrd",
	}

	ety, err = s.Add(ety)

	assert.Nil(t, err)
	assert.NotNil(t, ety)
	assert.Equal(t, int64(1), ety.Version)

	ety.Data = "Hello EventStore"

	ety, err = s.Append(ety)
	assert.Nil(t, err)
	assert.NotNil(t, ety)
	assert.Equal(t, int64(2), ety.Version)

	version, err := s.GetLatestVersionNumber("1234")
	assert.Nil(t, err)
	assert.Equal(t, int64(2), version)
}

func TestGetLatestVersionNumberMissingEntity(t *testing.T) {
	s := NewStore(*storageAccountFlag, *storageAccountKeyFlag, "t6")
	err := s.Init()
	assert.Nil(t, err)

	defer destroyTestData(s.(*tablestore))

	version, err := s.GetLatestVersionNumber("1234")
	assert.NotNil(t, err)
	assert.Equal(t, int64(0), version)
}

func TestGetByVersion(t *testing.T) {
	s := NewStore(*storageAccountFlag, *storageAccountKeyFlag, "t7")
	err := s.Init()
	assert.Nil(t, err)

	defer destroyTestData(s.(*tablestore))

	ety := &store.Entity{
		ID:   "1234",
		Data: "Hello World",
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

	ety, err = s.Append(ety)
	assert.Nil(t, err)
	assert.NotNil(t, ety)
	assert.Equal(t, int64(2), ety.Version)

	res, err = s.GetByVersion("1234", 2)
	assert.Nil(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, "Hello EventStore", res.Data.(string))
}
