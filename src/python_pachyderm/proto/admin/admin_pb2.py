# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: client/admin/admin.proto

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
from python_pachyderm.proto.admin.v1_7.pfs import pfs_pb2 as client_dot_admin_dot_v1__7_dot_pfs_dot_pfs__pb2
from python_pachyderm.proto.admin.v1_7.pps import pps_pb2 as client_dot_admin_dot_v1__7_dot_pps_dot_pps__pb2
from python_pachyderm.proto.admin.v1_8.pfs import pfs_pb2 as client_dot_admin_dot_v1__8_dot_pfs_dot_pfs__pb2
from python_pachyderm.proto.admin.v1_8.pps import pps_pb2 as client_dot_admin_dot_v1__8_dot_pps_dot_pps__pb2
from python_pachyderm.proto.admin.v1_9.pfs import pfs_pb2 as client_dot_admin_dot_v1__9_dot_pfs_dot_pfs__pb2
from python_pachyderm.proto.admin.v1_9.pps import pps_pb2 as client_dot_admin_dot_v1__9_dot_pps_dot_pps__pb2
from python_pachyderm.proto.admin.v1_10.pfs import pfs_pb2 as client_dot_admin_dot_v1__10_dot_pfs_dot_pfs__pb2
from python_pachyderm.proto.admin.v1_10.pps import pps_pb2 as client_dot_admin_dot_v1__10_dot_pps_dot_pps__pb2
from python_pachyderm.proto.pfs import pfs_pb2 as client_dot_pfs_dot_pfs__pb2
from python_pachyderm.proto.pps import pps_pb2 as client_dot_pps_dot_pps__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='client/admin/admin.proto',
  package='admin',
  syntax='proto3',
  serialized_options=b'Z/github.com/pachyderm/pachyderm/src/client/admin',
  serialized_pb=b'\n\x18\x63lient/admin/admin.proto\x12\x05\x61\x64min\x1a\x1bgoogle/protobuf/empty.proto\x1a\x1f\x63lient/admin/v1_7/pfs/pfs.proto\x1a\x1f\x63lient/admin/v1_7/pps/pps.proto\x1a\x1f\x63lient/admin/v1_8/pfs/pfs.proto\x1a\x1f\x63lient/admin/v1_8/pps/pps.proto\x1a\x1f\x63lient/admin/v1_9/pfs/pfs.proto\x1a\x1f\x63lient/admin/v1_9/pps/pps.proto\x1a client/admin/v1_10/pfs/pfs.proto\x1a client/admin/v1_10/pps/pps.proto\x1a\x14\x63lient/pfs/pfs.proto\x1a\x14\x63lient/pps/pps.proto\"\x91\x02\n\x05Op1_7\x12)\n\x06object\x18\x02 \x01(\x0b\x32\x19.pfs_1_7.PutObjectRequest\x12&\n\x03tag\x18\x03 \x01(\x0b\x32\x19.pfs_1_7.TagObjectRequest\x12(\n\x04repo\x18\x04 \x01(\x0b\x32\x1a.pfs_1_7.CreateRepoRequest\x12+\n\x06\x63ommit\x18\x05 \x01(\x0b\x32\x1b.pfs_1_7.BuildCommitRequest\x12,\n\x06\x62ranch\x18\x06 \x01(\x0b\x32\x1c.pfs_1_7.CreateBranchRequest\x12\x30\n\x08pipeline\x18\x07 \x01(\x0b\x32\x1e.pps_1_7.CreatePipelineRequest\"\x91\x02\n\x05Op1_8\x12)\n\x06object\x18\x02 \x01(\x0b\x32\x19.pfs_1_8.PutObjectRequest\x12&\n\x03tag\x18\x03 \x01(\x0b\x32\x19.pfs_1_8.TagObjectRequest\x12(\n\x04repo\x18\x04 \x01(\x0b\x32\x1a.pfs_1_8.CreateRepoRequest\x12+\n\x06\x63ommit\x18\x05 \x01(\x0b\x32\x1b.pfs_1_8.BuildCommitRequest\x12,\n\x06\x62ranch\x18\x06 \x01(\x0b\x32\x1c.pfs_1_8.CreateBranchRequest\x12\x30\n\x08pipeline\x18\x07 \x01(\x0b\x32\x1e.pps_1_8.CreatePipelineRequest\"\x97\x03\n\x05Op1_9\x12)\n\x06object\x18\x02 \x01(\x0b\x32\x19.pfs_1_9.PutObjectRequest\x12\x33\n\rcreate_object\x18\t \x01(\x0b\x32\x1c.pfs_1_9.CreateObjectRequest\x12&\n\x03tag\x18\x03 \x01(\x0b\x32\x19.pfs_1_9.TagObjectRequest\x12\'\n\x05\x62lock\x18\n \x01(\x0b\x32\x18.pfs_1_9.PutBlockRequest\x12(\n\x04repo\x18\x04 \x01(\x0b\x32\x1a.pfs_1_9.CreateRepoRequest\x12+\n\x06\x63ommit\x18\x05 \x01(\x0b\x32\x1b.pfs_1_9.BuildCommitRequest\x12,\n\x06\x62ranch\x18\x06 \x01(\x0b\x32\x1c.pfs_1_9.CreateBranchRequest\x12\x30\n\x08pipeline\x18\x07 \x01(\x0b\x32\x1e.pps_1_9.CreatePipelineRequest\x12&\n\x03job\x18\x08 \x01(\x0b\x32\x19.pps_1_9.CreateJobRequest\"\xa1\x03\n\x06Op1_10\x12*\n\x06object\x18\x02 \x01(\x0b\x32\x1a.pfs_1_10.PutObjectRequest\x12\x34\n\rcreate_object\x18\t \x01(\x0b\x32\x1d.pfs_1_10.CreateObjectRequest\x12\'\n\x03tag\x18\x03 \x01(\x0b\x32\x1a.pfs_1_10.TagObjectRequest\x12(\n\x05\x62lock\x18\n \x01(\x0b\x32\x19.pfs_1_10.PutBlockRequest\x12)\n\x04repo\x18\x04 \x01(\x0b\x32\x1b.pfs_1_10.CreateRepoRequest\x12,\n\x06\x63ommit\x18\x05 \x01(\x0b\x32\x1c.pfs_1_10.BuildCommitRequest\x12-\n\x06\x62ranch\x18\x06 \x01(\x0b\x32\x1d.pfs_1_10.CreateBranchRequest\x12\x31\n\x08pipeline\x18\x07 \x01(\x0b\x32\x1f.pps_1_10.CreatePipelineRequest\x12\'\n\x03job\x18\x08 \x01(\x0b\x32\x1a.pps_1_10.CreateJobRequest\"\xf4\x02\n\x06Op1_11\x12%\n\x06object\x18\x02 \x01(\x0b\x32\x15.pfs.PutObjectRequest\x12/\n\rcreate_object\x18\t \x01(\x0b\x32\x18.pfs.CreateObjectRequest\x12\"\n\x03tag\x18\x03 \x01(\x0b\x32\x15.pfs.TagObjectRequest\x12#\n\x05\x62lock\x18\n \x01(\x0b\x32\x14.pfs.PutBlockRequest\x12$\n\x04repo\x18\x04 \x01(\x0b\x32\x16.pfs.CreateRepoRequest\x12\'\n\x06\x63ommit\x18\x05 \x01(\x0b\x32\x17.pfs.BuildCommitRequest\x12(\n\x06\x62ranch\x18\x06 \x01(\x0b\x32\x18.pfs.CreateBranchRequest\x12,\n\x08pipeline\x18\x07 \x01(\x0b\x32\x1a.pps.CreatePipelineRequest\x12\"\n\x03job\x18\x08 \x01(\x0b\x32\x15.pps.CreateJobRequest\"\x99\x01\n\x02Op\x12\x1b\n\x05op1_7\x18\x01 \x01(\x0b\x32\x0c.admin.Op1_7\x12\x1b\n\x05op1_8\x18\x02 \x01(\x0b\x32\x0c.admin.Op1_8\x12\x1b\n\x05op1_9\x18\x03 \x01(\x0b\x32\x0c.admin.Op1_9\x12\x1d\n\x06op1_10\x18\x04 \x01(\x0b\x32\r.admin.Op1_10\x12\x1d\n\x06op1_11\x18\x05 \x01(\x0b\x32\r.admin.Op1_11\"Y\n\x0e\x45xtractRequest\x12\x0b\n\x03URL\x18\x01 \x01(\t\x12\x12\n\nno_objects\x18\x02 \x01(\x08\x12\x10\n\x08no_repos\x18\x03 \x01(\x08\x12\x14\n\x0cno_pipelines\x18\x04 \x01(\x08\"9\n\x16\x45xtractPipelineRequest\x12\x1f\n\x08pipeline\x18\x01 \x01(\x0b\x32\r.pps.Pipeline\"4\n\x0eRestoreRequest\x12\x15\n\x02op\x18\x01 \x01(\x0b\x32\t.admin.Op\x12\x0b\n\x03URL\x18\x02 \x01(\t\"0\n\x0b\x43lusterInfo\x12\n\n\x02id\x18\x01 \x01(\t\x12\x15\n\rdeployment_id\x18\x02 \x01(\t2\xf3\x01\n\x03\x41PI\x12/\n\x07\x45xtract\x12\x15.admin.ExtractRequest\x1a\t.admin.Op\"\x00\x30\x01\x12=\n\x0f\x45xtractPipeline\x12\x1d.admin.ExtractPipelineRequest\x1a\t.admin.Op\"\x00\x12<\n\x07Restore\x12\x15.admin.RestoreRequest\x1a\x16.google.protobuf.Empty\"\x00(\x01\x12>\n\x0eInspectCluster\x12\x16.google.protobuf.Empty\x1a\x12.admin.ClusterInfo\"\x00\x42\x31Z/github.com/pachyderm/pachyderm/src/client/adminb\x06proto3'
  ,
  dependencies=[google_dot_protobuf_dot_empty__pb2.DESCRIPTOR,client_dot_admin_dot_v1__7_dot_pfs_dot_pfs__pb2.DESCRIPTOR,client_dot_admin_dot_v1__7_dot_pps_dot_pps__pb2.DESCRIPTOR,client_dot_admin_dot_v1__8_dot_pfs_dot_pfs__pb2.DESCRIPTOR,client_dot_admin_dot_v1__8_dot_pps_dot_pps__pb2.DESCRIPTOR,client_dot_admin_dot_v1__9_dot_pfs_dot_pfs__pb2.DESCRIPTOR,client_dot_admin_dot_v1__9_dot_pps_dot_pps__pb2.DESCRIPTOR,client_dot_admin_dot_v1__10_dot_pfs_dot_pfs__pb2.DESCRIPTOR,client_dot_admin_dot_v1__10_dot_pps_dot_pps__pb2.DESCRIPTOR,client_dot_pfs_dot_pfs__pb2.DESCRIPTOR,client_dot_pps_dot_pps__pb2.DESCRIPTOR,])




_OP1_7 = _descriptor.Descriptor(
  name='Op1_7',
  full_name='admin.Op1_7',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='object', full_name='admin.Op1_7.object', index=0,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='tag', full_name='admin.Op1_7.tag', index=1,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='repo', full_name='admin.Op1_7.repo', index=2,
      number=4, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='commit', full_name='admin.Op1_7.commit', index=3,
      number=5, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='branch', full_name='admin.Op1_7.branch', index=4,
      number=6, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='pipeline', full_name='admin.Op1_7.pipeline', index=5,
      number=7, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
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
  serialized_start=375,
  serialized_end=648,
)


_OP1_8 = _descriptor.Descriptor(
  name='Op1_8',
  full_name='admin.Op1_8',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='object', full_name='admin.Op1_8.object', index=0,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='tag', full_name='admin.Op1_8.tag', index=1,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='repo', full_name='admin.Op1_8.repo', index=2,
      number=4, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='commit', full_name='admin.Op1_8.commit', index=3,
      number=5, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='branch', full_name='admin.Op1_8.branch', index=4,
      number=6, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='pipeline', full_name='admin.Op1_8.pipeline', index=5,
      number=7, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
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
  serialized_start=651,
  serialized_end=924,
)


_OP1_9 = _descriptor.Descriptor(
  name='Op1_9',
  full_name='admin.Op1_9',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='object', full_name='admin.Op1_9.object', index=0,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='create_object', full_name='admin.Op1_9.create_object', index=1,
      number=9, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='tag', full_name='admin.Op1_9.tag', index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='block', full_name='admin.Op1_9.block', index=3,
      number=10, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='repo', full_name='admin.Op1_9.repo', index=4,
      number=4, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='commit', full_name='admin.Op1_9.commit', index=5,
      number=5, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='branch', full_name='admin.Op1_9.branch', index=6,
      number=6, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='pipeline', full_name='admin.Op1_9.pipeline', index=7,
      number=7, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='job', full_name='admin.Op1_9.job', index=8,
      number=8, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
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
  serialized_start=927,
  serialized_end=1334,
)


_OP1_10 = _descriptor.Descriptor(
  name='Op1_10',
  full_name='admin.Op1_10',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='object', full_name='admin.Op1_10.object', index=0,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='create_object', full_name='admin.Op1_10.create_object', index=1,
      number=9, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='tag', full_name='admin.Op1_10.tag', index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='block', full_name='admin.Op1_10.block', index=3,
      number=10, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='repo', full_name='admin.Op1_10.repo', index=4,
      number=4, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='commit', full_name='admin.Op1_10.commit', index=5,
      number=5, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='branch', full_name='admin.Op1_10.branch', index=6,
      number=6, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='pipeline', full_name='admin.Op1_10.pipeline', index=7,
      number=7, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='job', full_name='admin.Op1_10.job', index=8,
      number=8, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
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
  serialized_start=1337,
  serialized_end=1754,
)


_OP1_11 = _descriptor.Descriptor(
  name='Op1_11',
  full_name='admin.Op1_11',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='object', full_name='admin.Op1_11.object', index=0,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='create_object', full_name='admin.Op1_11.create_object', index=1,
      number=9, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='tag', full_name='admin.Op1_11.tag', index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='block', full_name='admin.Op1_11.block', index=3,
      number=10, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='repo', full_name='admin.Op1_11.repo', index=4,
      number=4, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='commit', full_name='admin.Op1_11.commit', index=5,
      number=5, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='branch', full_name='admin.Op1_11.branch', index=6,
      number=6, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='pipeline', full_name='admin.Op1_11.pipeline', index=7,
      number=7, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='job', full_name='admin.Op1_11.job', index=8,
      number=8, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
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
  serialized_start=1757,
  serialized_end=2129,
)


_OP = _descriptor.Descriptor(
  name='Op',
  full_name='admin.Op',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='op1_7', full_name='admin.Op.op1_7', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='op1_8', full_name='admin.Op.op1_8', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='op1_9', full_name='admin.Op.op1_9', index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='op1_10', full_name='admin.Op.op1_10', index=3,
      number=4, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='op1_11', full_name='admin.Op.op1_11', index=4,
      number=5, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
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
  serialized_start=2132,
  serialized_end=2285,
)


_EXTRACTREQUEST = _descriptor.Descriptor(
  name='ExtractRequest',
  full_name='admin.ExtractRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='URL', full_name='admin.ExtractRequest.URL', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='no_objects', full_name='admin.ExtractRequest.no_objects', index=1,
      number=2, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='no_repos', full_name='admin.ExtractRequest.no_repos', index=2,
      number=3, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='no_pipelines', full_name='admin.ExtractRequest.no_pipelines', index=3,
      number=4, type=8, cpp_type=7, label=1,
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
  serialized_start=2287,
  serialized_end=2376,
)


_EXTRACTPIPELINEREQUEST = _descriptor.Descriptor(
  name='ExtractPipelineRequest',
  full_name='admin.ExtractPipelineRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='pipeline', full_name='admin.ExtractPipelineRequest.pipeline', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
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
  serialized_start=2378,
  serialized_end=2435,
)


_RESTOREREQUEST = _descriptor.Descriptor(
  name='RestoreRequest',
  full_name='admin.RestoreRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='op', full_name='admin.RestoreRequest.op', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='URL', full_name='admin.RestoreRequest.URL', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
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
  serialized_start=2437,
  serialized_end=2489,
)


_CLUSTERINFO = _descriptor.Descriptor(
  name='ClusterInfo',
  full_name='admin.ClusterInfo',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='id', full_name='admin.ClusterInfo.id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='deployment_id', full_name='admin.ClusterInfo.deployment_id', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
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
  serialized_start=2491,
  serialized_end=2539,
)

_OP1_7.fields_by_name['object'].message_type = client_dot_admin_dot_v1__7_dot_pfs_dot_pfs__pb2._PUTOBJECTREQUEST
_OP1_7.fields_by_name['tag'].message_type = client_dot_admin_dot_v1__7_dot_pfs_dot_pfs__pb2._TAGOBJECTREQUEST
_OP1_7.fields_by_name['repo'].message_type = client_dot_admin_dot_v1__7_dot_pfs_dot_pfs__pb2._CREATEREPOREQUEST
_OP1_7.fields_by_name['commit'].message_type = client_dot_admin_dot_v1__7_dot_pfs_dot_pfs__pb2._BUILDCOMMITREQUEST
_OP1_7.fields_by_name['branch'].message_type = client_dot_admin_dot_v1__7_dot_pfs_dot_pfs__pb2._CREATEBRANCHREQUEST
_OP1_7.fields_by_name['pipeline'].message_type = client_dot_admin_dot_v1__7_dot_pps_dot_pps__pb2._CREATEPIPELINEREQUEST
_OP1_8.fields_by_name['object'].message_type = client_dot_admin_dot_v1__8_dot_pfs_dot_pfs__pb2._PUTOBJECTREQUEST
_OP1_8.fields_by_name['tag'].message_type = client_dot_admin_dot_v1__8_dot_pfs_dot_pfs__pb2._TAGOBJECTREQUEST
_OP1_8.fields_by_name['repo'].message_type = client_dot_admin_dot_v1__8_dot_pfs_dot_pfs__pb2._CREATEREPOREQUEST
_OP1_8.fields_by_name['commit'].message_type = client_dot_admin_dot_v1__8_dot_pfs_dot_pfs__pb2._BUILDCOMMITREQUEST
_OP1_8.fields_by_name['branch'].message_type = client_dot_admin_dot_v1__8_dot_pfs_dot_pfs__pb2._CREATEBRANCHREQUEST
_OP1_8.fields_by_name['pipeline'].message_type = client_dot_admin_dot_v1__8_dot_pps_dot_pps__pb2._CREATEPIPELINEREQUEST
_OP1_9.fields_by_name['object'].message_type = client_dot_admin_dot_v1__9_dot_pfs_dot_pfs__pb2._PUTOBJECTREQUEST
_OP1_9.fields_by_name['create_object'].message_type = client_dot_admin_dot_v1__9_dot_pfs_dot_pfs__pb2._CREATEOBJECTREQUEST
_OP1_9.fields_by_name['tag'].message_type = client_dot_admin_dot_v1__9_dot_pfs_dot_pfs__pb2._TAGOBJECTREQUEST
_OP1_9.fields_by_name['block'].message_type = client_dot_admin_dot_v1__9_dot_pfs_dot_pfs__pb2._PUTBLOCKREQUEST
_OP1_9.fields_by_name['repo'].message_type = client_dot_admin_dot_v1__9_dot_pfs_dot_pfs__pb2._CREATEREPOREQUEST
_OP1_9.fields_by_name['commit'].message_type = client_dot_admin_dot_v1__9_dot_pfs_dot_pfs__pb2._BUILDCOMMITREQUEST
_OP1_9.fields_by_name['branch'].message_type = client_dot_admin_dot_v1__9_dot_pfs_dot_pfs__pb2._CREATEBRANCHREQUEST
_OP1_9.fields_by_name['pipeline'].message_type = client_dot_admin_dot_v1__9_dot_pps_dot_pps__pb2._CREATEPIPELINEREQUEST
_OP1_9.fields_by_name['job'].message_type = client_dot_admin_dot_v1__9_dot_pps_dot_pps__pb2._CREATEJOBREQUEST
_OP1_10.fields_by_name['object'].message_type = client_dot_admin_dot_v1__10_dot_pfs_dot_pfs__pb2._PUTOBJECTREQUEST
_OP1_10.fields_by_name['create_object'].message_type = client_dot_admin_dot_v1__10_dot_pfs_dot_pfs__pb2._CREATEOBJECTREQUEST
_OP1_10.fields_by_name['tag'].message_type = client_dot_admin_dot_v1__10_dot_pfs_dot_pfs__pb2._TAGOBJECTREQUEST
_OP1_10.fields_by_name['block'].message_type = client_dot_admin_dot_v1__10_dot_pfs_dot_pfs__pb2._PUTBLOCKREQUEST
_OP1_10.fields_by_name['repo'].message_type = client_dot_admin_dot_v1__10_dot_pfs_dot_pfs__pb2._CREATEREPOREQUEST
_OP1_10.fields_by_name['commit'].message_type = client_dot_admin_dot_v1__10_dot_pfs_dot_pfs__pb2._BUILDCOMMITREQUEST
_OP1_10.fields_by_name['branch'].message_type = client_dot_admin_dot_v1__10_dot_pfs_dot_pfs__pb2._CREATEBRANCHREQUEST
_OP1_10.fields_by_name['pipeline'].message_type = client_dot_admin_dot_v1__10_dot_pps_dot_pps__pb2._CREATEPIPELINEREQUEST
_OP1_10.fields_by_name['job'].message_type = client_dot_admin_dot_v1__10_dot_pps_dot_pps__pb2._CREATEJOBREQUEST
_OP1_11.fields_by_name['object'].message_type = client_dot_pfs_dot_pfs__pb2._PUTOBJECTREQUEST
_OP1_11.fields_by_name['create_object'].message_type = client_dot_pfs_dot_pfs__pb2._CREATEOBJECTREQUEST
_OP1_11.fields_by_name['tag'].message_type = client_dot_pfs_dot_pfs__pb2._TAGOBJECTREQUEST
_OP1_11.fields_by_name['block'].message_type = client_dot_pfs_dot_pfs__pb2._PUTBLOCKREQUEST
_OP1_11.fields_by_name['repo'].message_type = client_dot_pfs_dot_pfs__pb2._CREATEREPOREQUEST
_OP1_11.fields_by_name['commit'].message_type = client_dot_pfs_dot_pfs__pb2._BUILDCOMMITREQUEST
_OP1_11.fields_by_name['branch'].message_type = client_dot_pfs_dot_pfs__pb2._CREATEBRANCHREQUEST
_OP1_11.fields_by_name['pipeline'].message_type = client_dot_pps_dot_pps__pb2._CREATEPIPELINEREQUEST
_OP1_11.fields_by_name['job'].message_type = client_dot_pps_dot_pps__pb2._CREATEJOBREQUEST
_OP.fields_by_name['op1_7'].message_type = _OP1_7
_OP.fields_by_name['op1_8'].message_type = _OP1_8
_OP.fields_by_name['op1_9'].message_type = _OP1_9
_OP.fields_by_name['op1_10'].message_type = _OP1_10
_OP.fields_by_name['op1_11'].message_type = _OP1_11
_EXTRACTPIPELINEREQUEST.fields_by_name['pipeline'].message_type = client_dot_pps_dot_pps__pb2._PIPELINE
_RESTOREREQUEST.fields_by_name['op'].message_type = _OP
DESCRIPTOR.message_types_by_name['Op1_7'] = _OP1_7
DESCRIPTOR.message_types_by_name['Op1_8'] = _OP1_8
DESCRIPTOR.message_types_by_name['Op1_9'] = _OP1_9
DESCRIPTOR.message_types_by_name['Op1_10'] = _OP1_10
DESCRIPTOR.message_types_by_name['Op1_11'] = _OP1_11
DESCRIPTOR.message_types_by_name['Op'] = _OP
DESCRIPTOR.message_types_by_name['ExtractRequest'] = _EXTRACTREQUEST
DESCRIPTOR.message_types_by_name['ExtractPipelineRequest'] = _EXTRACTPIPELINEREQUEST
DESCRIPTOR.message_types_by_name['RestoreRequest'] = _RESTOREREQUEST
DESCRIPTOR.message_types_by_name['ClusterInfo'] = _CLUSTERINFO
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

Op1_7 = _reflection.GeneratedProtocolMessageType('Op1_7', (_message.Message,), {
  'DESCRIPTOR' : _OP1_7,
  '__module__' : 'client.admin.admin_pb2'
  # @@protoc_insertion_point(class_scope:admin.Op1_7)
  })
_sym_db.RegisterMessage(Op1_7)

Op1_8 = _reflection.GeneratedProtocolMessageType('Op1_8', (_message.Message,), {
  'DESCRIPTOR' : _OP1_8,
  '__module__' : 'client.admin.admin_pb2'
  # @@protoc_insertion_point(class_scope:admin.Op1_8)
  })
_sym_db.RegisterMessage(Op1_8)

Op1_9 = _reflection.GeneratedProtocolMessageType('Op1_9', (_message.Message,), {
  'DESCRIPTOR' : _OP1_9,
  '__module__' : 'client.admin.admin_pb2'
  # @@protoc_insertion_point(class_scope:admin.Op1_9)
  })
_sym_db.RegisterMessage(Op1_9)

Op1_10 = _reflection.GeneratedProtocolMessageType('Op1_10', (_message.Message,), {
  'DESCRIPTOR' : _OP1_10,
  '__module__' : 'client.admin.admin_pb2'
  # @@protoc_insertion_point(class_scope:admin.Op1_10)
  })
_sym_db.RegisterMessage(Op1_10)

Op1_11 = _reflection.GeneratedProtocolMessageType('Op1_11', (_message.Message,), {
  'DESCRIPTOR' : _OP1_11,
  '__module__' : 'client.admin.admin_pb2'
  # @@protoc_insertion_point(class_scope:admin.Op1_11)
  })
_sym_db.RegisterMessage(Op1_11)

Op = _reflection.GeneratedProtocolMessageType('Op', (_message.Message,), {
  'DESCRIPTOR' : _OP,
  '__module__' : 'client.admin.admin_pb2'
  # @@protoc_insertion_point(class_scope:admin.Op)
  })
_sym_db.RegisterMessage(Op)

ExtractRequest = _reflection.GeneratedProtocolMessageType('ExtractRequest', (_message.Message,), {
  'DESCRIPTOR' : _EXTRACTREQUEST,
  '__module__' : 'client.admin.admin_pb2'
  # @@protoc_insertion_point(class_scope:admin.ExtractRequest)
  })
_sym_db.RegisterMessage(ExtractRequest)

ExtractPipelineRequest = _reflection.GeneratedProtocolMessageType('ExtractPipelineRequest', (_message.Message,), {
  'DESCRIPTOR' : _EXTRACTPIPELINEREQUEST,
  '__module__' : 'client.admin.admin_pb2'
  # @@protoc_insertion_point(class_scope:admin.ExtractPipelineRequest)
  })
_sym_db.RegisterMessage(ExtractPipelineRequest)

RestoreRequest = _reflection.GeneratedProtocolMessageType('RestoreRequest', (_message.Message,), {
  'DESCRIPTOR' : _RESTOREREQUEST,
  '__module__' : 'client.admin.admin_pb2'
  # @@protoc_insertion_point(class_scope:admin.RestoreRequest)
  })
_sym_db.RegisterMessage(RestoreRequest)

ClusterInfo = _reflection.GeneratedProtocolMessageType('ClusterInfo', (_message.Message,), {
  'DESCRIPTOR' : _CLUSTERINFO,
  '__module__' : 'client.admin.admin_pb2'
  # @@protoc_insertion_point(class_scope:admin.ClusterInfo)
  })
_sym_db.RegisterMessage(ClusterInfo)


DESCRIPTOR._options = None

_API = _descriptor.ServiceDescriptor(
  name='API',
  full_name='admin.API',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  serialized_start=2542,
  serialized_end=2785,
  methods=[
  _descriptor.MethodDescriptor(
    name='Extract',
    full_name='admin.API.Extract',
    index=0,
    containing_service=None,
    input_type=_EXTRACTREQUEST,
    output_type=_OP,
    serialized_options=None,
  ),
  _descriptor.MethodDescriptor(
    name='ExtractPipeline',
    full_name='admin.API.ExtractPipeline',
    index=1,
    containing_service=None,
    input_type=_EXTRACTPIPELINEREQUEST,
    output_type=_OP,
    serialized_options=None,
  ),
  _descriptor.MethodDescriptor(
    name='Restore',
    full_name='admin.API.Restore',
    index=2,
    containing_service=None,
    input_type=_RESTOREREQUEST,
    output_type=google_dot_protobuf_dot_empty__pb2._EMPTY,
    serialized_options=None,
  ),
  _descriptor.MethodDescriptor(
    name='InspectCluster',
    full_name='admin.API.InspectCluster',
    index=3,
    containing_service=None,
    input_type=google_dot_protobuf_dot_empty__pb2._EMPTY,
    output_type=_CLUSTERINFO,
    serialized_options=None,
  ),
])
_sym_db.RegisterServiceDescriptor(_API)

DESCRIPTOR.services_by_name['API'] = _API

# @@protoc_insertion_point(module_scope)
