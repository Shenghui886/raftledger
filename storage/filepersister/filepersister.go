package filepersister

import (
	"encoding/json"
	"os"

	"github.com/Shenghui886/raftledger/storage"
)

type FilePersister struct {
	filePath string
}

func New(filePath string) *FilePersister {
	return &FilePersister{
		filePath: filePath,
	}
}

func (fp *FilePersister) Save(state storage.PersistedState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	tmp := fp.filePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, fp.filePath)
}

func (fp *FilePersister) Load() (storage.PersistedState, error) {
	data, err := os.ReadFile(fp.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return storage.PersistedState{VotedFor: -1}, nil
		}
		return storage.PersistedState{}, err
	}
	var state storage.PersistedState
	err = json.Unmarshal(data, &state)
	return state, err
}

func (fp *FilePersister) Close() error {
	return nil
}
