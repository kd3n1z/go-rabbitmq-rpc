package mqrpc

import (
	"encoding/json"
	"errors"
)

type RpcSerializer interface {
	Serialize(any) ([]byte, error)
}

type RawSerializer struct{}

func (RawSerializer) Serialize(data any) ([]byte, error) {
	result, ok := data.([]byte)

	if ok != true {
		return nil, errors.New("data must be []byte")
	}

	return result, nil
}

type JsonSerializer struct{}

func (JsonSerializer) Serialize(data any) ([]byte, error) {
	return json.Marshal(data)
}
