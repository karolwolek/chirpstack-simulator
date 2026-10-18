package config

import (
	"encoding/hex"
	"reflect"

	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
)

func DecodeHexPayload() mapstructure.DecodeHookFuncType {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{},
	) (interface{}, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}

		if t != reflect.TypeOf(HexBytes{}) {
			return data, nil
		}

		bytes, err := hex.DecodeString(data.(string))
		if err != nil {
			return nil, errors.Wrap(err, "mine decode payload error")
		}

		return HexBytes(bytes), nil
	}
}
