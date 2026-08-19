package config

import (
	"reflect"

	bmwconfig "github.com/fujin-io/fujin/public/plugins/middleware/bind/config"
	cmwconfig "github.com/fujin-io/fujin/public/plugins/middleware/connector/config"
)

// CloneConnectorConfig returns a deep copy suitable for immutable generation storage
// or caller-owned mutation. Opaque plugin-local leaves such as channels and functions
// are retained by identity; maps, slices, pointers, interfaces, arrays, and structs are copied.
func CloneConnectorConfig(original ConnectorConfig) (ConnectorConfig, error) {
	return ConnectorConfig{
		Type:                 original.Type,
		Overridable:          append([]string(nil), original.Overridable...),
		BindMiddlewares:      cloneValue(reflect.ValueOf(original.BindMiddlewares)).Interface().([]bmwconfig.Config),
		ConnectorMiddlewares: cloneValue(reflect.ValueOf(original.ConnectorMiddlewares)).Interface().([]cmwconfig.Config),
		Settings:             cloneInterface(original.Settings),
	}, nil
}

func cloneInterface(value any) any {
	if value == nil {
		return nil
	}
	return cloneValue(reflect.ValueOf(value)).Interface()
}

func cloneValue(value reflect.Value) reflect.Value {
	if !value.IsValid() {
		return value
	}
	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		clone := cloneValue(value.Elem())
		result := reflect.New(value.Type()).Elem()
		result.Set(clone)
		return result
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		clone := reflect.New(value.Type().Elem())
		clone.Elem().Set(cloneValue(value.Elem()))
		return clone
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		clone := reflect.MakeMapWithSize(value.Type(), value.Len())
		iterator := value.MapRange()
		for iterator.Next() {
			clone.SetMapIndex(iterator.Key(), cloneValue(iterator.Value()))
		}
		return clone
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		clone := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for i := 0; i < value.Len(); i++ {
			clone.Index(i).Set(cloneValue(value.Index(i)))
		}
		return clone
	case reflect.Array:
		clone := reflect.New(value.Type()).Elem()
		for i := 0; i < value.Len(); i++ {
			clone.Index(i).Set(cloneValue(value.Index(i)))
		}
		return clone
	case reflect.Struct:
		clone := reflect.New(value.Type()).Elem()
		clone.Set(value)
		for i := 0; i < value.NumField(); i++ {
			if value.Type().Field(i).PkgPath == "" {
				clone.Field(i).Set(cloneValue(value.Field(i)))
			}
		}
		return clone
	default:
		return value
	}
}
