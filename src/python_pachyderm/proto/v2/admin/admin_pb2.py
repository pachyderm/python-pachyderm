# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: python_pachyderm/proto/v2/admin/admin.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='python_pachyderm/proto/v2/admin/admin.proto',
  package='admin_v2',
  syntax='proto3',
  serialized_options=b'Z+github.com/pachyderm/pachyderm/v2/src/admin',
  create_key=_descriptor._internal_create_key,
  serialized_pb=b'\n+python_pachyderm/proto/v2/admin/admin.proto\x12\x08\x61\x64min_v2\x1a\x1bgoogle/protobuf/empty.proto\"0\n\x0b\x43lusterInfo\x12\n\n\x02id\x18\x01 \x01(\t\x12\x15\n\rdeployment_id\x18\x02 \x01(\t2H\n\x03\x41PI\x12\x41\n\x0eInspectCluster\x12\x16.google.protobuf.Empty\x1a\x15.admin_v2.ClusterInfo\"\x00\x42-Z+github.com/pachyderm/pachyderm/v2/src/adminb\x06proto3'
  ,
  dependencies=[google_dot_protobuf_dot_empty__pb2.DESCRIPTOR,])




_CLUSTERINFO = _descriptor.Descriptor(
  name='ClusterInfo',
  full_name='admin_v2.ClusterInfo',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='id', full_name='admin_v2.ClusterInfo.id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='deployment_id', full_name='admin_v2.ClusterInfo.deployment_id', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=86,
  serialized_end=134,
)

DESCRIPTOR.message_types_by_name['ClusterInfo'] = _CLUSTERINFO
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

ClusterInfo = _reflection.GeneratedProtocolMessageType('ClusterInfo', (_message.Message,), {
  'DESCRIPTOR' : _CLUSTERINFO,
  '__module__' : 'python_pachyderm.proto.v2.admin.admin_pb2'
  # @@protoc_insertion_point(class_scope:admin_v2.ClusterInfo)
  })
_sym_db.RegisterMessage(ClusterInfo)


DESCRIPTOR._options = None

_API = _descriptor.ServiceDescriptor(
  name='API',
  full_name='admin_v2.API',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_start=136,
  serialized_end=208,
  methods=[
  _descriptor.MethodDescriptor(
    name='InspectCluster',
    full_name='admin_v2.API.InspectCluster',
    index=0,
    containing_service=None,
    input_type=google_dot_protobuf_dot_empty__pb2._EMPTY,
    output_type=_CLUSTERINFO,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
])
_sym_db.RegisterServiceDescriptor(_API)

DESCRIPTOR.services_by_name['API'] = _API

# @@protoc_insertion_point(module_scope)
