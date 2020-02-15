package listeners

import (
	"github.com/pkg/errors"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"

	envoy_listener "github.com/envoyproxy/go-control-plane/envoy/api/v2/listener"
)

func UpdateFilterConfig(filterChain *envoy_listener.FilterChain, filterName string, updateFunc func(proto.Message) error) error {
	for i, filter := range filterChain.Filters {
		if filter.Name == filterName {
			if filter.GetTypedConfig() == nil {
				return errors.Errorf("filters[%d]: config cannot be 'nil'", i)
			}

			var dany ptypes.DynamicAny
			if err := ptypes.UnmarshalAny(filter.GetTypedConfig(), &dany); err != nil {
				return err
			}
			if err := updateFunc(dany.Message); err != nil {
				return err
			}

			pbst, err := ptypes.MarshalAny(dany.Message)
			if err != nil {
				return err
			}

			filter.ConfigType = &envoy_listener.Filter_TypedConfig{
				TypedConfig: pbst,
			}
		}
	}
	return nil
}

func NewUnexpectedFilterConfigTypeError(actual, expected proto.Message) error {
	return errors.Errorf("filter config has unexpected type: expected %T, got %T", expected, actual)
}
