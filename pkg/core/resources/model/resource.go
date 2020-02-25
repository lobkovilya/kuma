package model

import (
	"fmt"
	"reflect"
	"time"

	"github.com/golang/protobuf/proto"
)

const (
	DefaultMesh = "default"
)

var (
	// DimensionalResourceNameUnsupported is a convenience constant
	// that is meant to make source code more readable.
	DimensionalResourceNameUnsupported = DimensionalResourceName(nil)
)

type ResourceKey struct {
	Mesh string
	Name string
}

type Resource interface {
	GetType() ResourceType
	GetMeta() ResourceMeta
	SetMeta(ResourceMeta)
	GetSpec() ResourceSpec
	SetSpec(ResourceSpec) error
	Validate() error
}

type ResourceType string

// DimensionalResourceName represents a composite resource name.
//
// E.g., name of a Kubernetes resource consists of a namespace component
// and a name component that is local to that namespace.
//
// Technically, DimensionalResourceName is a mapping between
// a component identifier and a component value, e.g.
//
//   "k8s.kuma.io/namespace" => "my-namespace"
//   "k8s.kuma.io/name"      => "my-policy"
//
// Component identifier must be considered a part of user-facing Kuma API.
// In other words, it is supposed to be visible to users and should not be changed lightly.
//
// Component identifier might have any value, however, it's preferable
// to choose one that is intuitive to users of that particular environment.
// E.g., on Kubernetes component identifiers should use a label name format,
// like in "k8s.kuma.io/namespace" and "k8s.kuma.io/name".
type DimensionalResourceName map[string]string

type ResourceMeta interface {
	GetName() string
	GetDimensionalName() DimensionalResourceName
	GetVersion() string
	GetMesh() string
	GetCreationTime() time.Time
	GetModificationTime() time.Time
}

func MetaToResourceKey(meta ResourceMeta) ResourceKey {
	if meta == nil {
		return ResourceKey{}
	}
	return ResourceKey{
		Mesh: meta.GetMesh(),
		Name: meta.GetName(),
	}
}

type ResourceSpec interface {
	// all resources must be defined via Protobuf
	proto.Message
}

type ResourceList interface {
	GetItemType() ResourceType
	GetItems() []Resource
	NewItem() Resource
	AddItem(Resource) error
}

func ErrorInvalidItemType(expected, actual interface{}) error {
	return fmt.Errorf("Invalid argument type: expected=%q got=%q", reflect.TypeOf(expected), reflect.TypeOf(actual))
}
