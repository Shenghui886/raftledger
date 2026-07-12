package storage

type PersistedState struct {
	CurrentTerm uint64  `json:"current_term"`
	VotedFor    int     `json:"voted_for"`
	Blocks      []Block `json:"blocks"`
}
