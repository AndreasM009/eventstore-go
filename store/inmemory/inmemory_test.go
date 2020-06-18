package inmemory

import (
	"testing"

	"github.com/AndreasM009/eventstore-impl/store"
	"github.com/stretchr/testify/assert"
)

var testMetadata = store.Metadata{
	Properties: make(map[string]string),
}

func TestAdd(t *testing.T) {
	s := NewStore()
	err := s.Init(testMetadata)
	assert.Nil(t, err)

	ety := &store.Entity{
		ID:      "1",
		Version: 0,
		Data:    "Hello World",
	}

	res, err := s.Add(ety)

	assert.Nil(t, err)
	assert.NotNil(t, res)

	res, err = s.Add(ety)

	assert.NotNil(t, err)
	assert.Nil(t, res)
}

func TestAppend(t *testing.T) {
	s := NewStore()
	err := s.Init(testMetadata)
	assert.Nil(t, err)

	ety := &store.Entity{
		ID:      "1",
		Version: 0,
		Data:    "Hello World",
	}

	res, err := s.Add(ety)

	assert.Nil(t, err)
	assert.NotNil(t, res)

	ety.Data = "Hello World!"
	res, err = s.Append(ety, store.Optimistic)

	assert.Nil(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, int64(2), res.Version)

	res.Version = 1

	res, err = s.Append(res, store.Optimistic)

	assert.NotNil(t, err)
	assert.Nil(t, res)
}

func TestAppendMissingEntity(t *testing.T) {
	s := NewStore()
	err := s.Init(testMetadata)
	assert.Nil(t, err)

	ety := &store.Entity{
		ID:      "1",
		Version: 0,
		Data:    "Hello World",
	}

	res, err := s.Append(ety, store.Optimistic)

	assert.NotNil(t, err)
	assert.Nil(t, res)
}

func TestGetVersionNumber(t *testing.T) {
	s := NewStore()
	err := s.Init(testMetadata)
	assert.Nil(t, err)

	ety := &store.Entity{
		ID:      "1",
		Version: 0,
		Data:    "Hello World",
	}

	e, err := s.Add(ety)
	assert.Nil(t, err)
	assert.NotNil(t, e)

	version, err := s.GetLatestVersionNumber(ety.ID)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), version)

	ety.Data = "Hello World!"
	e, err = s.Append(ety, store.Optimistic)
	assert.Nil(t, err)
	assert.NotNil(t, e)

	version, err = s.GetLatestVersionNumber(ety.ID)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), version)
}

func TestGetVersionNumberMissingEntity(t *testing.T) {
	s := NewStore()
	err := s.Init(testMetadata)
	assert.Nil(t, err)

	ety := &store.Entity{
		ID:      "1",
		Version: 0,
		Data:    "Hello World",
	}

	version, err := s.GetLatestVersionNumber(ety.ID)

	assert.NotNil(t, err)
	assert.Equal(t, int64(0), version)
}

func TestGetByVersion(t *testing.T) {
	s := NewStore()
	err := s.Init(testMetadata)
	assert.Nil(t, err)

	ety := &store.Entity{
		ID:      "1",
		Version: 0,
		Data:    "Hello World",
	}

	e, err := s.Add(ety)
	assert.Nil(t, err)
	assert.NotNil(t, e)

	res, err := s.GetByVersion(ety.ID, ety.Version)

	assert.Nil(t, err)
	assert.NotNil(t, res)
	// pointers are not equal
	assert.True(t, res != ety)
	assert.Equal(t, res.Data.(string), ety.Data.(string))

	ety2 := &store.Entity{
		ID:      "1",
		Version: 1,
		Data:    "Hello World!",
	}

	e, err = s.Append(ety2, store.Optimistic)
	assert.Nil(t, err)
	assert.NotNil(t, e)

	res, err = s.GetByVersion(ety2.ID, 1)
	assert.Nil(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, res.Data.(string), "Hello World")

	res, err = s.GetByVersion(ety2.ID, 2)
	assert.Nil(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, res.Data.(string), "Hello World!")
}

func TestGetByVerisonMissingEntity(t *testing.T) {
	s := NewStore()
	err := s.Init(testMetadata)
	assert.Nil(t, err)

	res, err := s.GetByVersion("1", 1)
	assert.NotNil(t, err)
	assert.Nil(t, res)
}

func TestGetByVersionMissingVersion(t *testing.T) {
	s := NewStore()
	err := s.Init(testMetadata)
	assert.Nil(t, err)

	ety := &store.Entity{
		ID:      "1",
		Version: 0,
		Data:    "Hello World",
	}

	_, _ = s.Add(ety)

	res, err := s.GetByVersion("1", 2)
	assert.NotNil(t, err)
	assert.Nil(t, res)
}
