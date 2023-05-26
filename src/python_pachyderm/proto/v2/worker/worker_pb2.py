# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: python_pachyderm/proto/v2/worker/worker.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from python_pachyderm.proto.v2.pps import pps_pb2 as python__pachyderm_dot_proto_dot_v2_dot_pps_dot_pps__pb2
from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='python_pachyderm/proto/v2/worker/worker.proto',
  package='pachyderm.worker',
  syntax='proto3',
  serialized_options=b'Z,github.com/pachyderm/pachyderm/v2/src/worker',
  create_key=_descriptor._internal_create_key,
  serialized_pb=b'\n-python_pachyderm/proto/v2/worker/worker.proto\x12\x10pachyderm.worker\x1a\'python_pachyderm/proto/v2/pps/pps.proto\x1a\x1bgoogle/protobuf/empty.proto\"5\n\rCancelRequest\x12\x0e\n\x06job_id\x18\x01 \x01(\t\x12\x14\n\x0c\x64\x61ta_filters\x18\x02 \x03(\t\"!\n\x0e\x43\x61ncelResponse\x12\x0f\n\x07success\x18\x01 \x01(\x08\"!\n\x10NextDatumRequest\x12\r\n\x05\x65rror\x18\x01 \x01(\t\" \n\x11NextDatumResponse\x12\x0b\n\x03\x65nv\x18\x01 \x03(\t2\xe9\x01\n\x06Worker\x12\x38\n\x06Status\x12\x16.google.protobuf.Empty\x1a\x14.pps_v2.WorkerStatus\"\x00\x12M\n\x06\x43\x61ncel\x12\x1f.pachyderm.worker.CancelRequest\x1a .pachyderm.worker.CancelResponse\"\x00\x12V\n\tNextDatum\x12\".pachyderm.worker.NextDatumRequest\x1a#.pachyderm.worker.NextDatumResponse\"\x00\x42.Z,github.com/pachyderm/pachyderm/v2/src/workerb\x06proto3'
  ,
  dependencies=[python__pachyderm_dot_proto_dot_v2_dot_pps_dot_pps__pb2.DESCRIPTOR,google_dot_protobuf_dot_empty__pb2.DESCRIPTOR,])




_CANCELREQUEST = _descriptor.Descriptor(
  name='CancelRequest',
  full_name='pachyderm.worker.CancelRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='job_id', full_name='pachyderm.worker.CancelRequest.job_id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='data_filters', full_name='pachyderm.worker.CancelRequest.data_filters', index=1,
      number=2, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
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
  serialized_start=137,
  serialized_end=190,
)


_CANCELRESPONSE = _descriptor.Descriptor(
  name='CancelResponse',
  full_name='pachyderm.worker.CancelResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='success', full_name='pachyderm.worker.CancelResponse.success', index=0,
      number=1, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
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
  serialized_start=192,
  serialized_end=225,
)


_NEXTDATUMREQUEST = _descriptor.Descriptor(
  name='NextDatumRequest',
  full_name='pachyderm.worker.NextDatumRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='error', full_name='pachyderm.worker.NextDatumRequest.error', index=0,
      number=1, type=9, cpp_type=9, label=1,
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
  serialized_start=227,
  serialized_end=260,
)


_NEXTDATUMRESPONSE = _descriptor.Descriptor(
  name='NextDatumResponse',
  full_name='pachyderm.worker.NextDatumResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='env', full_name='pachyderm.worker.NextDatumResponse.env', index=0,
      number=1, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
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
  serialized_start=262,
  serialized_end=294,
)

DESCRIPTOR.message_types_by_name['CancelRequest'] = _CANCELREQUEST
DESCRIPTOR.message_types_by_name['CancelResponse'] = _CANCELRESPONSE
DESCRIPTOR.message_types_by_name['NextDatumRequest'] = _NEXTDATUMREQUEST
DESCRIPTOR.message_types_by_name['NextDatumResponse'] = _NEXTDATUMRESPONSE
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

CancelRequest = _reflection.GeneratedProtocolMessageType('CancelRequest', (_message.Message,), {
  'DESCRIPTOR' : _CANCELREQUEST,
  '__module__' : 'python_pachyderm.proto.v2.worker.worker_pb2'
  # @@protoc_insertion_point(class_scope:pachyderm.worker.CancelRequest)
  })
_sym_db.RegisterMessage(CancelRequest)

CancelResponse = _reflection.GeneratedProtocolMessageType('CancelResponse', (_message.Message,), {
  'DESCRIPTOR' : _CANCELRESPONSE,
  '__module__' : 'python_pachyderm.proto.v2.worker.worker_pb2'
  # @@protoc_insertion_point(class_scope:pachyderm.worker.CancelResponse)
  })
_sym_db.RegisterMessage(CancelResponse)

NextDatumRequest = _reflection.GeneratedProtocolMessageType('NextDatumRequest', (_message.Message,), {
  'DESCRIPTOR' : _NEXTDATUMREQUEST,
  '__module__' : 'python_pachyderm.proto.v2.worker.worker_pb2'
  # @@protoc_insertion_point(class_scope:pachyderm.worker.NextDatumRequest)
  })
_sym_db.RegisterMessage(NextDatumRequest)

NextDatumResponse = _reflection.GeneratedProtocolMessageType('NextDatumResponse', (_message.Message,), {
  'DESCRIPTOR' : _NEXTDATUMRESPONSE,
  '__module__' : 'python_pachyderm.proto.v2.worker.worker_pb2'
  # @@protoc_insertion_point(class_scope:pachyderm.worker.NextDatumResponse)
  })
_sym_db.RegisterMessage(NextDatumResponse)


DESCRIPTOR._options = None

_WORKER = _descriptor.ServiceDescriptor(
  name='Worker',
  full_name='pachyderm.worker.Worker',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_start=297,
  serialized_end=530,
  methods=[
  _descriptor.MethodDescriptor(
    name='Status',
    full_name='pachyderm.worker.Worker.Status',
    index=0,
    containing_service=None,
    input_type=google_dot_protobuf_dot_empty__pb2._EMPTY,
    output_type=python__pachyderm_dot_proto_dot_v2_dot_pps_dot_pps__pb2._WORKERSTATUS,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='Cancel',
    full_name='pachyderm.worker.Worker.Cancel',
    index=1,
    containing_service=None,
    input_type=_CANCELREQUEST,
    output_type=_CANCELRESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='NextDatum',
    full_name='pachyderm.worker.Worker.NextDatum',
    index=2,
    containing_service=None,
    input_type=_NEXTDATUMREQUEST,
    output_type=_NEXTDATUMRESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
])
_sym_db.RegisterServiceDescriptor(_WORKER)

DESCRIPTOR.services_by_name['Worker'] = _WORKER

# @@protoc_insertion_point(module_scope)