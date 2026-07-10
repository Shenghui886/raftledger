package storage

import "encoding/json"

type Block struct {
	Index     uint64      `json:"index"`
	Term      uint64      `json:"term"`
	Timestamp uint64      `json:"timestamp"`
	Data      Transaction `json:"data"`
}

func (b Block) Serialize() ([]byte, error) {
	return json.Marshal(b)
}

func DeserializeBlock(data []byte) (blk Block, err error) {
	err = json.Unmarshal(data, &blk)
	return
}
