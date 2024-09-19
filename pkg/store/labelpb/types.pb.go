// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.34.2
// 	protoc        v3.20.0
// source: store/labelpb/types.proto

package labelpb

import (
	reflect "reflect"
	sync "sync"

	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Label struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name  string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Value string `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *Label) Reset() {
	*x = Label{}
	if protoimpl.UnsafeEnabled {
		mi := &file_store_labelpb_types_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Label) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Label) ProtoMessage() {}

func (x *Label) ProtoReflect() protoreflect.Message {
	mi := &file_store_labelpb_types_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Label.ProtoReflect.Descriptor instead.
func (*Label) Descriptor() ([]byte, []int) {
	return file_store_labelpb_types_proto_rawDescGZIP(), []int{0}
}

func (x *Label) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Label) GetValue() string {
	if x != nil {
		return x.Value
	}
	return ""
}

type LabelSet struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Labels []*Label `protobuf:"bytes,1,rep,name=labels,proto3" json:"labels,omitempty"`
}

func (x *LabelSet) Reset() {
	*x = LabelSet{}
	if protoimpl.UnsafeEnabled {
		mi := &file_store_labelpb_types_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *LabelSet) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LabelSet) ProtoMessage() {}

func (x *LabelSet) ProtoReflect() protoreflect.Message {
	mi := &file_store_labelpb_types_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LabelSet.ProtoReflect.Descriptor instead.
func (*LabelSet) Descriptor() ([]byte, []int) {
	return file_store_labelpb_types_proto_rawDescGZIP(), []int{1}
}

func (x *LabelSet) GetLabels() []*Label {
	if x != nil {
		return x.Labels
	}
	return nil
}

var File_store_labelpb_types_proto protoreflect.FileDescriptor

var file_store_labelpb_types_proto_rawDesc = []byte{
	0x0a, 0x19, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x2f, 0x6c, 0x61, 0x62, 0x65, 0x6c, 0x70, 0x62, 0x2f,
	0x74, 0x79, 0x70, 0x65, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x06, 0x74, 0x68, 0x61,
	0x6e, 0x6f, 0x73, 0x22, 0x31, 0x0a, 0x05, 0x4c, 0x61, 0x62, 0x65, 0x6c, 0x12, 0x12, 0x0a, 0x04,
	0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65,
	0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x31, 0x0a, 0x08, 0x4c, 0x61, 0x62, 0x65, 0x6c, 0x53,
	0x65, 0x74, 0x12, 0x25, 0x0a, 0x06, 0x6c, 0x61, 0x62, 0x65, 0x6c, 0x73, 0x18, 0x01, 0x20, 0x03,
	0x28, 0x0b, 0x32, 0x0d, 0x2e, 0x74, 0x68, 0x61, 0x6e, 0x6f, 0x73, 0x2e, 0x4c, 0x61, 0x62, 0x65,
	0x6c, 0x52, 0x06, 0x6c, 0x61, 0x62, 0x65, 0x6c, 0x73, 0x42, 0x2f, 0x5a, 0x2d, 0x67, 0x69, 0x74,
	0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x74, 0x68, 0x61, 0x6e, 0x6f, 0x73, 0x2d, 0x69,
	0x6f, 0x2f, 0x74, 0x68, 0x61, 0x6e, 0x6f, 0x73, 0x2f, 0x70, 0x6b, 0x67, 0x2f, 0x73, 0x74, 0x6f,
	0x72, 0x65, 0x2f, 0x6c, 0x61, 0x62, 0x65, 0x6c, 0x70, 0x62, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x33,
}

var (
	file_store_labelpb_types_proto_rawDescOnce sync.Once
	file_store_labelpb_types_proto_rawDescData = file_store_labelpb_types_proto_rawDesc
)

func file_store_labelpb_types_proto_rawDescGZIP() []byte {
	file_store_labelpb_types_proto_rawDescOnce.Do(func() {
		file_store_labelpb_types_proto_rawDescData = protoimpl.X.CompressGZIP(file_store_labelpb_types_proto_rawDescData)
	})
	return file_store_labelpb_types_proto_rawDescData
}

var file_store_labelpb_types_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_store_labelpb_types_proto_goTypes = []any{
	(*Label)(nil),    // 0: thanos.Label
	(*LabelSet)(nil), // 1: thanos.LabelSet
}
var file_store_labelpb_types_proto_depIdxs = []int32{
	0, // 0: thanos.LabelSet.labels:type_name -> thanos.Label
	1, // [1:1] is the sub-list for method output_type
	1, // [1:1] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_store_labelpb_types_proto_init() }
func file_store_labelpb_types_proto_init() {
	if File_store_labelpb_types_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_store_labelpb_types_proto_msgTypes[0].Exporter = func(v any, i int) any {
			switch v := v.(*Label); i {
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
		file_store_labelpb_types_proto_msgTypes[1].Exporter = func(v any, i int) any {
			switch v := v.(*LabelSet); i {
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
			RawDescriptor: file_store_labelpb_types_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_store_labelpb_types_proto_goTypes,
		DependencyIndexes: file_store_labelpb_types_proto_depIdxs,
		MessageInfos:      file_store_labelpb_types_proto_msgTypes,
	}.Build()
	File_store_labelpb_types_proto = out.File
	file_store_labelpb_types_proto_rawDesc = nil
	file_store_labelpb_types_proto_goTypes = nil
	file_store_labelpb_types_proto_depIdxs = nil
}
