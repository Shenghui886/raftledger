package storage

import "encoding/json"

type Transaction struct {
	Data []byte `json:"data"`
}

func (t Transaction) Serialize() ([]byte, error) {
	return json.Marshal(t)
}

func DeserializeTransaction(data []byte) (t Transaction, err error) {
	err = json.Unmarshal(data, &t)
	return
}
