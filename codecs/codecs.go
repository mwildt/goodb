// contains codec implementation to serialize data
package codecs

import (
	"encoding/base64"
	"encoding/json"
)

type Codec[T any] interface {
	Encode(T) ([]byte, error)
	Decode([]byte) (T, error)
}

func NewBase64JsonCodec[T any]() Codec[T] {
	return NewBase64WrapperCodec[T](NewJsonCodec[T]())
}

type JsonCodec[T any] struct{}

func NewJsonCodec[T any]() *JsonCodec[T] {
	return &JsonCodec[T]{}
}
func (codec JsonCodec[T]) Encode(value T) (bytes []byte, err error) {
	return json.Marshal(value)
}

func (codec JsonCodec[T]) Decode(bytes []byte) (value T, err error) {
	err = json.Unmarshal(bytes, &value)
	return value, err
}

type Base64WrapperCodec[T any] struct {
	delegate Codec[T]
	encoding *base64.Encoding
}

func NewBase64WrapperCodec[T any](delegate Codec[T]) *Base64WrapperCodec[T] {
	return &Base64WrapperCodec[T]{
		delegate: delegate,
		encoding: base64.RawStdEncoding,
	}
}

func (codec Base64WrapperCodec[T]) Encode(value T) (bytes []byte, err error) {
	if encoded, err := codec.delegate.Encode(value); err != nil {
		return bytes, err
	} else {
		bytes = make([]byte, codec.encoding.EncodedLen(len(encoded)))
		codec.encoding.Encode(bytes, encoded)
		return bytes, err
	}
}

func (codec Base64WrapperCodec[T]) Decode(bytes []byte) (value T, err error) {
	decoded := make([]byte, codec.encoding.DecodedLen(len(bytes)))
	if _, err = codec.encoding.Decode(decoded, bytes); err != nil {
		return value, err
	} else {
		return codec.delegate.Decode(decoded)
	}
}
