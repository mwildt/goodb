package messagelog

import (
	"encoding/base64"
	"encoding/json"
)

type Encoder[T any] func(T) ([]byte, error)

type Decoder[T any] func([]byte) (T, error)

func B64JsonEncoder[T any](value T) (data []byte, err error) {
	jsonData, err := json.Marshal(value)
	if err != nil {
		return data, err
	}
	encoder := base64.RawStdEncoding
	data = make([]byte, encoder.EncodedLen(len(jsonData)))
	encoder.Encode(data, jsonData)
	return data, err
}

func B64JsonDecoder[T any](data []byte) (value T, err error) {
	encoding := base64.RawStdEncoding
	jsonValue := make([]byte, encoding.DecodedLen(len(data)))
	_, err = encoding.Decode(jsonValue, data)
	if err != nil {
		return value, err
	}
	err = json.Unmarshal(jsonValue, &value)
	if err != nil {
		return value, err
	}
	return value, err
}
