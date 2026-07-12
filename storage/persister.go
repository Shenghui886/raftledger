package storage

type Persister interface {
	Save(state PersistedState) error
	Load() (PersistedState, error)
	Close() error
}
