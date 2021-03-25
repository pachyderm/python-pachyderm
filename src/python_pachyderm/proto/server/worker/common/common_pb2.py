# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: src/server/worker/common/common.proto

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from python_pachyderm.proto.pfs import pfs_pb2 as src_dot_pfs_dot_pfs__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='src/server/worker/common/common.proto',
  package='common',
  syntax='proto3',
  serialized_options=b'Z:github.com/pachyderm/pachyderm/v2/src/server/worker/common',
  serialized_pb=b'\n%src/server/worker/common/common.proto\x12\x06\x63ommon\x1a\x11src/pfs/pfs.proto\"\xe2\x01\n\x05Input\x12 \n\tfile_info\x18\x01 \x01(\x0b\x32\r.pfs.FileInfo\x12\"\n\rparent_commit\x18\x05 \x01(\x0b\x32\x0b.pfs.Commit\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x0f\n\x07join_on\x18\x08 \x01(\t\x12\x12\n\nouter_join\x18\x0b \x01(\x08\x12\x10\n\x08group_by\x18\n \x01(\t\x12\x0c\n\x04lazy\x18\x03 \x01(\x08\x12\x0e\n\x06\x62ranch\x18\x04 \x01(\t\x12\x0f\n\x07git_url\x18\x06 \x01(\t\x12\x13\n\x0b\x65mpty_files\x18\x07 \x01(\x08\x12\n\n\x02s3\x18\t \x01(\x08\x42<Z:github.com/pachyderm/pachyderm/v2/src/server/worker/commonb\x06proto3'
  ,
  dependencies=[src_dot_pfs_dot_pfs__pb2.DESCRIPTOR,])




_INPUT = _descriptor.Descriptor(
  name='Input',
  full_name='common.Input',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='file_info', full_name='common.Input.file_info', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='parent_commit', full_name='common.Input.parent_commit', index=1,
      number=5, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='name', full_name='common.Input.name', index=2,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='join_on', full_name='common.Input.join_on', index=3,
      number=8, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='outer_join', full_name='common.Input.outer_join', index=4,
      number=11, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='group_by', full_name='common.Input.group_by', index=5,
      number=10, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='lazy', full_name='common.Input.lazy', index=6,
      number=3, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='branch', full_name='common.Input.branch', index=7,
      number=4, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='git_url', full_name='common.Input.git_url', index=8,
      number=6, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='empty_files', full_name='common.Input.empty_files', index=9,
      number=7, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='s3', full_name='common.Input.s3', index=10,
      number=9, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
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
  serialized_start=69,
  serialized_end=295,
)

_INPUT.fields_by_name['file_info'].message_type = src_dot_pfs_dot_pfs__pb2._FILEINFO
_INPUT.fields_by_name['parent_commit'].message_type = src_dot_pfs_dot_pfs__pb2._COMMIT
DESCRIPTOR.message_types_by_name['Input'] = _INPUT
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

Input = _reflection.GeneratedProtocolMessageType('Input', (_message.Message,), {
  'DESCRIPTOR' : _INPUT,
  '__module__' : 'src.server.worker.common.common_pb2'
  # @@protoc_insertion_point(class_scope:common.Input)
  })
_sym_db.RegisterMessage(Input)


DESCRIPTOR._options = None
# @@protoc_insertion_point(module_scope)