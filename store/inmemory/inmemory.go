package inmemory

import (
	"fmt"
	"sync"

	"github.com/AndreasM009/eventstore-go/store"
)

type inmemory struct {
	entities map[string]map[int64]*store.Entity
	versions map[string]int64
	mutex    sync.Mutex
}

// NewStore creates a new in memory store
func NewStore() store.EventStore {
	return &inmemory{}
}

func (s *inmemory) Init(metadata store.Metadata) error {

	s.versions = make(map[string]int64)
	s.entities = make(map[string]map[int64]*store.Entity)
	return nil
}

func (s *inmemory) Add(entity *store.Entity) (*store.Entity, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, exists := s.versions[entity.ID]; exists {
		return nil, fmt.Errorf("An entity with id %s already exists", entity.ID)
	}

	entity.Version = 1
	s.versions[entity.ID] = int64(1)

	s.entities[entity.ID] = make(map[int64]*store.Entity)
	s.entities[entity.ID][entity.Version] = s.clone(entity)
	return entity, nil
}

func (s *inmemory) Append(entity *store.Entity, concurrency store.ConcurrencyControl) (*store.Entity, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	version, exists := s.versions[entity.ID]

	if !exists {
		return nil, fmt.Errorf("An entity with id %s already exists", entity.ID)
	}

	if version != entity.Version {
		return nil, fmt.Errorf("Entity %s has gone stale, newer version already available (OOL)", entity.ID)
	}

	version++
	entity.Version = version
	s.versions[entity.ID] = version
	s.entities[entity.ID][version] = s.clone(entity)
	return entity, nil
}

func (s *inmemory) GetLatestVersionNumber(id string) (int64, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	version, exists := s.versions[id]

	if !exists {
		return 0, fmt.Errorf("Entity with ID %s does not exist", id)
	}

	return version, nil
}

func (s *inmemory) GetByVersion(id string, version int64) (*store.Entity, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	ebv, exists := s.entities[id]
	if !exists {
		return nil, fmt.Errorf("Enity with ID %s does not exist", id)
	}

	entity, exists := ebv[version]

	if !exists {
		return nil, fmt.Errorf("Version %v of entity with ID %s does not exist", version, id)
	}

	return s.clone(entity), nil
}

func (s *inmemory) GetByVersionRange(id string, startVersion, endVersion int64) ([]store.Entity, error) {
	return nil, nil
}

func (s *inmemory) clone(entity *store.Entity) *store.Entity {
	return &store.Entity{
		ID:       entity.ID,
		Version:  entity.Version,
		Data:     entity.Data,
		Metadata: entity.Metadata,
	}
}
