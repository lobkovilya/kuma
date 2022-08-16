// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.26.0
// 	protoc        v3.20.0
// source: pkg/plugins/policies/meshtrafficpermission/api/v1alpha1/meshtrafficpermission.proto

package v1alpha1

import (
	v1alpha1 "github.com/kumahq/kuma/api/common/v1alpha1"
	_ "github.com/kumahq/kuma/api/mesh"
	_ "github.com/kumahq/protoc-gen-kumadoc/proto"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// MeshTrafficPermission defines permission for traffic between data planes prxoies.
type MeshTrafficPermission struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// TargetRef is a reference to the resource the policy takes an effect on.
	// The resource could be either a real store object or virtual resource defined inplace.
	TargetRef *v1alpha1.TargetRef `protobuf:"bytes,1,opt,name=targetRef,proto3" json:"targetRef,omitempty"`
	// From is a list of pairs – a group of clients and action applied for it
	From []*MeshTrafficPermission_From `protobuf:"bytes,2,rep,name=from,proto3" json:"from,omitempty"`
}

func (x *MeshTrafficPermission) Reset() {
	*x = MeshTrafficPermission{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MeshTrafficPermission) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MeshTrafficPermission) ProtoMessage() {}

func (x *MeshTrafficPermission) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MeshTrafficPermission.ProtoReflect.Descriptor instead.
func (*MeshTrafficPermission) Descriptor() ([]byte, []int) {
	return file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_rawDescGZIP(), []int{0}
}

func (x *MeshTrafficPermission) GetTargetRef() *v1alpha1.TargetRef {
	if x != nil {
		return x.TargetRef
	}
	return nil
}

func (x *MeshTrafficPermission) GetFrom() []*MeshTrafficPermission_From {
	if x != nil {
		return x.From
	}
	return nil
}

type MeshTrafficPermission_From struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// TargetRef is a reference to the resource that represents a group of clients.
	TargetRef *v1alpha1.TargetRef `protobuf:"bytes,1,opt,name=targetRef,proto3" json:"targetRef,omitempty"`
	// Default is a configuration specific to the group of clients referenced in 'targetRef'
	Default *MeshTrafficPermission_From_Default `protobuf:"bytes,2,opt,name=default,proto3" json:"default,omitempty"`
}

func (x *MeshTrafficPermission_From) Reset() {
	*x = MeshTrafficPermission_From{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MeshTrafficPermission_From) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MeshTrafficPermission_From) ProtoMessage() {}

func (x *MeshTrafficPermission_From) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MeshTrafficPermission_From.ProtoReflect.Descriptor instead.
func (*MeshTrafficPermission_From) Descriptor() ([]byte, []int) {
	return file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_rawDescGZIP(), []int{0, 0}
}

func (x *MeshTrafficPermission_From) GetTargetRef() *v1alpha1.TargetRef {
	if x != nil {
		return x.TargetRef
	}
	return nil
}

func (x *MeshTrafficPermission_From) GetDefault() *MeshTrafficPermission_From_Default {
	if x != nil {
		return x.Default
	}
	return nil
}

type MeshTrafficPermission_From_Default struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Action defines a behaviour for the specified group of clients:
	//  * ALLOW - lets the requests pass
	//  * DENY - blocks the requests
	//  * ALLOW_WITH_SHADOW_DENY - lets the requests pass but emits logs as if requests are denied
	//  * DENY_WITH_SHADOW_ALLOW - blocks the requests but emits logs as if requests are allowed
	// +kubebuilder:validation:Enum=ALLOW;DENY;ALLOW_WITH_SHADOW_DENY;DENY_WITH_SHADOW_ALLOW
	Action string `protobuf:"bytes,1,opt,name=action,proto3" json:"action,omitempty"`
}

func (x *MeshTrafficPermission_From_Default) Reset() {
	*x = MeshTrafficPermission_From_Default{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MeshTrafficPermission_From_Default) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MeshTrafficPermission_From_Default) ProtoMessage() {}

func (x *MeshTrafficPermission_From_Default) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MeshTrafficPermission_From_Default.ProtoReflect.Descriptor instead.
func (*MeshTrafficPermission_From_Default) Descriptor() ([]byte, []int) {
	return file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_rawDescGZIP(), []int{0, 0, 0}
}

func (x *MeshTrafficPermission_From_Default) GetAction() string {
	if x != nil {
		return x.Action
	}
	return ""
}

var File_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto protoreflect.FileDescriptor

var file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_rawDesc = []byte{
	0x0a, 0x53, 0x70, 0x6b, 0x67, 0x2f, 0x70, 0x6c, 0x75, 0x67, 0x69, 0x6e, 0x73, 0x2f, 0x70, 0x6f,
	0x6c, 0x69, 0x63, 0x69, 0x65, 0x73, 0x2f, 0x6d, 0x65, 0x73, 0x68, 0x74, 0x72, 0x61, 0x66, 0x66,
	0x69, 0x63, 0x70, 0x65, 0x72, 0x6d, 0x69, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x2f, 0x61, 0x70, 0x69,
	0x2f, 0x76, 0x31, 0x61, 0x6c, 0x70, 0x68, 0x61, 0x31, 0x2f, 0x6d, 0x65, 0x73, 0x68, 0x74, 0x72,
	0x61, 0x66, 0x66, 0x69, 0x63, 0x70, 0x65, 0x72, 0x6d, 0x69, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x34, 0x6b, 0x75, 0x6d, 0x61, 0x2e, 0x70, 0x6c, 0x75, 0x67,
	0x69, 0x6e, 0x73, 0x2e, 0x70, 0x6f, 0x6c, 0x69, 0x63, 0x69, 0x65, 0x73, 0x2e, 0x6d, 0x65, 0x73,
	0x68, 0x74, 0x72, 0x61, 0x66, 0x66, 0x69, 0x63, 0x70, 0x65, 0x72, 0x6d, 0x69, 0x73, 0x73, 0x69,
	0x6f, 0x6e, 0x2e, 0x76, 0x31, 0x61, 0x6c, 0x70, 0x68, 0x61, 0x31, 0x1a, 0x12, 0x6d, 0x65, 0x73,
	0x68, 0x2f, 0x6f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a,
	0x1f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2f, 0x76, 0x31, 0x61, 0x6c, 0x70, 0x68, 0x61, 0x31,
	0x2f, 0x74, 0x61, 0x72, 0x67, 0x65, 0x74, 0x72, 0x65, 0x66, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x1a, 0x0c, 0x63, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xa5,
	0x03, 0x0a, 0x15, 0x4d, 0x65, 0x73, 0x68, 0x54, 0x72, 0x61, 0x66, 0x66, 0x69, 0x63, 0x50, 0x65,
	0x72, 0x6d, 0x69, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x3d, 0x0a, 0x09, 0x74, 0x61, 0x72, 0x67,
	0x65, 0x74, 0x52, 0x65, 0x66, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1f, 0x2e, 0x6b, 0x75,
	0x6d, 0x61, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x76, 0x31, 0x61, 0x6c, 0x70, 0x68,
	0x61, 0x31, 0x2e, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74, 0x52, 0x65, 0x66, 0x52, 0x09, 0x74, 0x61,
	0x72, 0x67, 0x65, 0x74, 0x52, 0x65, 0x66, 0x12, 0x64, 0x0a, 0x04, 0x66, 0x72, 0x6f, 0x6d, 0x18,
	0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x50, 0x2e, 0x6b, 0x75, 0x6d, 0x61, 0x2e, 0x70, 0x6c, 0x75,
	0x67, 0x69, 0x6e, 0x73, 0x2e, 0x70, 0x6f, 0x6c, 0x69, 0x63, 0x69, 0x65, 0x73, 0x2e, 0x6d, 0x65,
	0x73, 0x68, 0x74, 0x72, 0x61, 0x66, 0x66, 0x69, 0x63, 0x70, 0x65, 0x72, 0x6d, 0x69, 0x73, 0x73,
	0x69, 0x6f, 0x6e, 0x2e, 0x76, 0x31, 0x61, 0x6c, 0x70, 0x68, 0x61, 0x31, 0x2e, 0x4d, 0x65, 0x73,
	0x68, 0x54, 0x72, 0x61, 0x66, 0x66, 0x69, 0x63, 0x50, 0x65, 0x72, 0x6d, 0x69, 0x73, 0x73, 0x69,
	0x6f, 0x6e, 0x2e, 0x46, 0x72, 0x6f, 0x6d, 0x52, 0x04, 0x66, 0x72, 0x6f, 0x6d, 0x1a, 0xdc, 0x01,
	0x0a, 0x04, 0x46, 0x72, 0x6f, 0x6d, 0x12, 0x3d, 0x0a, 0x09, 0x74, 0x61, 0x72, 0x67, 0x65, 0x74,
	0x52, 0x65, 0x66, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1f, 0x2e, 0x6b, 0x75, 0x6d, 0x61,
	0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x76, 0x31, 0x61, 0x6c, 0x70, 0x68, 0x61, 0x31,
	0x2e, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74, 0x52, 0x65, 0x66, 0x52, 0x09, 0x74, 0x61, 0x72, 0x67,
	0x65, 0x74, 0x52, 0x65, 0x66, 0x12, 0x72, 0x0a, 0x07, 0x64, 0x65, 0x66, 0x61, 0x75, 0x6c, 0x74,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x58, 0x2e, 0x6b, 0x75, 0x6d, 0x61, 0x2e, 0x70, 0x6c,
	0x75, 0x67, 0x69, 0x6e, 0x73, 0x2e, 0x70, 0x6f, 0x6c, 0x69, 0x63, 0x69, 0x65, 0x73, 0x2e, 0x6d,
	0x65, 0x73, 0x68, 0x74, 0x72, 0x61, 0x66, 0x66, 0x69, 0x63, 0x70, 0x65, 0x72, 0x6d, 0x69, 0x73,
	0x73, 0x69, 0x6f, 0x6e, 0x2e, 0x76, 0x31, 0x61, 0x6c, 0x70, 0x68, 0x61, 0x31, 0x2e, 0x4d, 0x65,
	0x73, 0x68, 0x54, 0x72, 0x61, 0x66, 0x66, 0x69, 0x63, 0x50, 0x65, 0x72, 0x6d, 0x69, 0x73, 0x73,
	0x69, 0x6f, 0x6e, 0x2e, 0x46, 0x72, 0x6f, 0x6d, 0x2e, 0x44, 0x65, 0x66, 0x61, 0x75, 0x6c, 0x74,
	0x52, 0x07, 0x64, 0x65, 0x66, 0x61, 0x75, 0x6c, 0x74, 0x1a, 0x21, 0x0a, 0x07, 0x44, 0x65, 0x66,
	0x61, 0x75, 0x6c, 0x74, 0x12, 0x16, 0x0a, 0x06, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x3a, 0x08, 0xb2, 0x8c,
	0x89, 0xa6, 0x01, 0x02, 0x08, 0x01, 0x42, 0x86, 0x01, 0x5a, 0x4e, 0x67, 0x69, 0x74, 0x68, 0x75,
	0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x6b, 0x75, 0x6d, 0x61, 0x68, 0x71, 0x2f, 0x6b, 0x75, 0x6d,
	0x61, 0x2f, 0x70, 0x6b, 0x67, 0x2f, 0x70, 0x6c, 0x75, 0x67, 0x69, 0x6e, 0x73, 0x2f, 0x70, 0x6f,
	0x6c, 0x69, 0x63, 0x69, 0x65, 0x73, 0x2f, 0x6d, 0x65, 0x73, 0x68, 0x74, 0x72, 0x61, 0x66, 0x66,
	0x69, 0x63, 0x70, 0x65, 0x72, 0x6d, 0x69, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x2f, 0x61, 0x70, 0x69,
	0x2f, 0x76, 0x31, 0x61, 0x6c, 0x70, 0x68, 0x61, 0x31, 0x8a, 0xb5, 0x18, 0x32, 0x50, 0x01, 0xa2,
	0x01, 0x15, 0x4d, 0x65, 0x73, 0x68, 0x54, 0x72, 0x61, 0x66, 0x66, 0x69, 0x63, 0x50, 0x65, 0x72,
	0x6d, 0x69, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0xf2, 0x01, 0x15, 0x6d, 0x65, 0x73, 0x68, 0x74, 0x72,
	0x61, 0x66, 0x66, 0x69, 0x63, 0x70, 0x65, 0x72, 0x6d, 0x69, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x62,
	0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_rawDescOnce sync.Once
	file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_rawDescData = file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_rawDesc
)

func file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_rawDescGZIP() []byte {
	file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_rawDescOnce.Do(func() {
		file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_rawDescData = protoimpl.X.CompressGZIP(file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_rawDescData)
	})
	return file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_rawDescData
}

var file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_goTypes = []interface{}{
	(*MeshTrafficPermission)(nil),              // 0: kuma.plugins.policies.meshtrafficpermission.v1alpha1.MeshTrafficPermission
	(*MeshTrafficPermission_From)(nil),         // 1: kuma.plugins.policies.meshtrafficpermission.v1alpha1.MeshTrafficPermission.From
	(*MeshTrafficPermission_From_Default)(nil), // 2: kuma.plugins.policies.meshtrafficpermission.v1alpha1.MeshTrafficPermission.From.Default
	(*v1alpha1.TargetRef)(nil),                 // 3: kuma.common.v1alpha1.TargetRef
}
var file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_depIdxs = []int32{
	3, // 0: kuma.plugins.policies.meshtrafficpermission.v1alpha1.MeshTrafficPermission.targetRef:type_name -> kuma.common.v1alpha1.TargetRef
	1, // 1: kuma.plugins.policies.meshtrafficpermission.v1alpha1.MeshTrafficPermission.from:type_name -> kuma.plugins.policies.meshtrafficpermission.v1alpha1.MeshTrafficPermission.From
	3, // 2: kuma.plugins.policies.meshtrafficpermission.v1alpha1.MeshTrafficPermission.From.targetRef:type_name -> kuma.common.v1alpha1.TargetRef
	2, // 3: kuma.plugins.policies.meshtrafficpermission.v1alpha1.MeshTrafficPermission.From.default:type_name -> kuma.plugins.policies.meshtrafficpermission.v1alpha1.MeshTrafficPermission.From.Default
	4, // [4:4] is the sub-list for method output_type
	4, // [4:4] is the sub-list for method input_type
	4, // [4:4] is the sub-list for extension type_name
	4, // [4:4] is the sub-list for extension extendee
	0, // [0:4] is the sub-list for field type_name
}

func init() {
	file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_init()
}
func file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_init() {
	if File_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MeshTrafficPermission); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MeshTrafficPermission_From); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MeshTrafficPermission_From_Default); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_goTypes,
		DependencyIndexes: file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_depIdxs,
		MessageInfos:      file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_msgTypes,
	}.Build()
	File_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto = out.File
	file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_rawDesc = nil
	file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_goTypes = nil
	file_pkg_plugins_policies_meshtrafficpermission_api_v1alpha1_meshtrafficpermission_proto_depIdxs = nil
}
