// Generated by tools/policy-gen.
// Run "make generate" to update this file.

// nolint:whitespace
package v1alpha1

import (
	_ "embed"
	"errors"
	"fmt"

	"k8s.io/kube-openapi/pkg/validation/spec"
	"sigs.k8s.io/yaml"

	"github.com/kumahq/kuma/pkg/core/resources/model"
)

//go:embed schema.yaml
var rawSchema []byte

func init() {
	var schema spec.Schema
	if err := yaml.Unmarshal(rawSchema, &schema); err != nil {
		panic(err)
	}
	rawSchema = nil
	MeshMetricResourceTypeDescriptor.Schema = &schema
}

const (
	MeshMetricType model.ResourceType = "MeshMetric"
)

var _ model.Resource = &MeshMetricResource{}

type MeshMetricResource struct {
	Meta model.ResourceMeta
	Spec *MeshMetric
}

func NewMeshMetricResource() *MeshMetricResource {
	return &MeshMetricResource{
		Spec: &MeshMetric{},
	}
}

func (t *MeshMetricResource) GetMeta() model.ResourceMeta {
	return t.Meta
}

func (t *MeshMetricResource) SetMeta(m model.ResourceMeta) {
	t.Meta = m
}

func (t *MeshMetricResource) GetSpec() model.ResourceSpec {
	return t.Spec
}

func (t *MeshMetricResource) SetSpec(spec model.ResourceSpec) error {
	protoType, ok := spec.(*MeshMetric)
	if !ok {
		return fmt.Errorf("invalid type %T for Spec", spec)
	} else {
		if protoType == nil {
			t.Spec = &MeshMetric{}
		} else {
			t.Spec = protoType
		}
		return nil
	}
}

func (t *MeshMetricResource) GetStatus() model.ResourceStatus {
	return nil
}

func (t *MeshMetricResource) SetStatus(_ model.ResourceStatus) error {
	return errors.New("status not supported")
}

func (t *MeshMetricResource) Descriptor() model.ResourceTypeDescriptor {
	return MeshMetricResourceTypeDescriptor
}

func (t *MeshMetricResource) Validate() error {
	if v, ok := interface{}(t).(interface{ validate() error }); !ok {
		return nil
	} else {
		return v.validate()
	}
}

var _ model.ResourceList = &MeshMetricResourceList{}

type MeshMetricResourceList struct {
	Items      []*MeshMetricResource
	Pagination model.Pagination
}

func (l *MeshMetricResourceList) GetItems() []model.Resource {
	res := make([]model.Resource, len(l.Items))
	for i, elem := range l.Items {
		res[i] = elem
	}
	return res
}

func (l *MeshMetricResourceList) GetItemType() model.ResourceType {
	return MeshMetricType
}

func (l *MeshMetricResourceList) NewItem() model.Resource {
	return NewMeshMetricResource()
}

func (l *MeshMetricResourceList) AddItem(r model.Resource) error {
	if trr, ok := r.(*MeshMetricResource); ok {
		l.Items = append(l.Items, trr)
		return nil
	} else {
		return model.ErrorInvalidItemType((*MeshMetricResource)(nil), r)
	}
}

func (l *MeshMetricResourceList) GetPagination() *model.Pagination {
	return &l.Pagination
}

func (l *MeshMetricResourceList) SetPagination(p model.Pagination) {
	l.Pagination = p
}

var MeshMetricResourceTypeDescriptor = model.ResourceTypeDescriptor{
	Name:                MeshMetricType,
	Resource:            NewMeshMetricResource(),
	ResourceList:        &MeshMetricResourceList{},
	Scope:               model.ScopeMesh,
	KDSFlags:            model.GlobalToAllZonesFlag | model.ZoneToGlobalFlag,
	WsPath:              "meshmetrics",
	KumactlArg:          "meshmetric",
	KumactlListArg:      "meshmetrics",
	AllowToInspect:      true,
	IsPolicy:            true,
	IsExperimental:      false,
	SingularDisplayName: "Mesh Metric",
	PluralDisplayName:   "Mesh Metrics",
	IsPluginOriginated:  true,
	IsTargetRefBased:    true,
	HasToTargetRef:      false,
	HasFromTargetRef:    false,
	HasStatus:           false,
}