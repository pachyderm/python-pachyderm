# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
import grpc

from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
from google.protobuf import wrappers_pb2 as google_dot_protobuf_dot_wrappers__pb2
from . import pfs_pb2 as pfs__pb2


class APIStub(object):
  # missing associated documentation comment in .proto file
  pass

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.CreateRepo = channel.unary_unary(
        '/pfs.API/CreateRepo',
        request_serializer=pfs__pb2.CreateRepoRequest.SerializeToString,
        response_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
        )
    self.InspectRepo = channel.unary_unary(
        '/pfs.API/InspectRepo',
        request_serializer=pfs__pb2.InspectRepoRequest.SerializeToString,
        response_deserializer=pfs__pb2.RepoInfo.FromString,
        )
    self.ListRepo = channel.unary_unary(
        '/pfs.API/ListRepo',
        request_serializer=pfs__pb2.ListRepoRequest.SerializeToString,
        response_deserializer=pfs__pb2.RepoInfos.FromString,
        )
    self.DeleteRepo = channel.unary_unary(
        '/pfs.API/DeleteRepo',
        request_serializer=pfs__pb2.DeleteRepoRequest.SerializeToString,
        response_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
        )
    self.StartCommit = channel.unary_unary(
        '/pfs.API/StartCommit',
        request_serializer=pfs__pb2.StartCommitRequest.SerializeToString,
        response_deserializer=pfs__pb2.Commit.FromString,
        )
    self.FinishCommit = channel.unary_unary(
        '/pfs.API/FinishCommit',
        request_serializer=pfs__pb2.FinishCommitRequest.SerializeToString,
        response_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
        )
    self.InspectCommit = channel.unary_unary(
        '/pfs.API/InspectCommit',
        request_serializer=pfs__pb2.InspectCommitRequest.SerializeToString,
        response_deserializer=pfs__pb2.CommitInfo.FromString,
        )
    self.ListCommit = channel.unary_unary(
        '/pfs.API/ListCommit',
        request_serializer=pfs__pb2.ListCommitRequest.SerializeToString,
        response_deserializer=pfs__pb2.CommitInfos.FromString,
        )
    self.DeleteCommit = channel.unary_unary(
        '/pfs.API/DeleteCommit',
        request_serializer=pfs__pb2.DeleteCommitRequest.SerializeToString,
        response_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
        )
    self.FlushCommit = channel.unary_stream(
        '/pfs.API/FlushCommit',
        request_serializer=pfs__pb2.FlushCommitRequest.SerializeToString,
        response_deserializer=pfs__pb2.CommitInfo.FromString,
        )
    self.SubscribeCommit = channel.unary_stream(
        '/pfs.API/SubscribeCommit',
        request_serializer=pfs__pb2.SubscribeCommitRequest.SerializeToString,
        response_deserializer=pfs__pb2.CommitInfo.FromString,
        )
    self.BuildCommit = channel.unary_unary(
        '/pfs.API/BuildCommit',
        request_serializer=pfs__pb2.BuildCommitRequest.SerializeToString,
        response_deserializer=pfs__pb2.Commit.FromString,
        )
    self.ListBranch = channel.unary_unary(
        '/pfs.API/ListBranch',
        request_serializer=pfs__pb2.ListBranchRequest.SerializeToString,
        response_deserializer=pfs__pb2.BranchInfos.FromString,
        )
    self.SetBranch = channel.unary_unary(
        '/pfs.API/SetBranch',
        request_serializer=pfs__pb2.SetBranchRequest.SerializeToString,
        response_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
        )
    self.DeleteBranch = channel.unary_unary(
        '/pfs.API/DeleteBranch',
        request_serializer=pfs__pb2.DeleteBranchRequest.SerializeToString,
        response_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
        )
    self.PutFile = channel.stream_unary(
        '/pfs.API/PutFile',
        request_serializer=pfs__pb2.PutFileRequest.SerializeToString,
        response_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
        )
    self.GetFile = channel.unary_stream(
        '/pfs.API/GetFile',
        request_serializer=pfs__pb2.GetFileRequest.SerializeToString,
        response_deserializer=google_dot_protobuf_dot_wrappers__pb2.BytesValue.FromString,
        )
    self.InspectFile = channel.unary_unary(
        '/pfs.API/InspectFile',
        request_serializer=pfs__pb2.InspectFileRequest.SerializeToString,
        response_deserializer=pfs__pb2.FileInfo.FromString,
        )
    self.ListFile = channel.unary_unary(
        '/pfs.API/ListFile',
        request_serializer=pfs__pb2.ListFileRequest.SerializeToString,
        response_deserializer=pfs__pb2.FileInfos.FromString,
        )
    self.GlobFile = channel.unary_unary(
        '/pfs.API/GlobFile',
        request_serializer=pfs__pb2.GlobFileRequest.SerializeToString,
        response_deserializer=pfs__pb2.FileInfos.FromString,
        )
    self.DiffFile = channel.unary_unary(
        '/pfs.API/DiffFile',
        request_serializer=pfs__pb2.DiffFileRequest.SerializeToString,
        response_deserializer=pfs__pb2.DiffFileResponse.FromString,
        )
    self.DeleteFile = channel.unary_unary(
        '/pfs.API/DeleteFile',
        request_serializer=pfs__pb2.DeleteFileRequest.SerializeToString,
        response_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
        )
    self.DeleteAll = channel.unary_unary(
        '/pfs.API/DeleteAll',
        request_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
        response_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
        )


class APIServicer(object):
  # missing associated documentation comment in .proto file
  pass

  def CreateRepo(self, request, context):
    """Repo rpcs
    CreateRepo creates a new repo.
    An error is returned if the repo already exists.
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def InspectRepo(self, request, context):
    """InspectRepo returns info about a repo.
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def ListRepo(self, request, context):
    """ListRepo returns info about all repos.
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def DeleteRepo(self, request, context):
    """DeleteRepo deletes a repo.
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def StartCommit(self, request, context):
    """Commit rpcs
    StartCommit creates a new write commit from a parent commit.
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def FinishCommit(self, request, context):
    """FinishCommit turns a write commit into a read commit.
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def InspectCommit(self, request, context):
    """InspectCommit returns the info about a commit.
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def ListCommit(self, request, context):
    """ListCommit returns info about all commits.
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def DeleteCommit(self, request, context):
    """DeleteCommit deletes a commit.
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def FlushCommit(self, request, context):
    """FlushCommit waits for downstream commits to finish
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def SubscribeCommit(self, request, context):
    """SubscribeCommit subscribes for new commits on a given branch
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def BuildCommit(self, request, context):
    """BuildCommit builds a commit that's backed by the given tree
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def ListBranch(self, request, context):
    """ListBranch returns info about the heads of branches.
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def SetBranch(self, request, context):
    """SetBranch assigns a commit and its ancestors to a branch.
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def DeleteBranch(self, request, context):
    """DeleteBranch deletes a branch; note that the commits still exist.
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def PutFile(self, request_iterator, context):
    """File rpcs
    PutFile writes the specified file to pfs.
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def GetFile(self, request, context):
    """GetFile returns a byte stream of the contents of the file.
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def InspectFile(self, request, context):
    """InspectFile returns info about a file.
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def ListFile(self, request, context):
    """ListFile returns info about all files.
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def GlobFile(self, request, context):
    """GlobFile returns info about all files.
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def DiffFile(self, request, context):
    """DiffFile returns the differences between 2 paths at 2 commits.
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def DeleteFile(self, request, context):
    """DeleteFile deletes a file.
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def DeleteAll(self, request, context):
    """DeleteAll deletes everything
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')


def add_APIServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'CreateRepo': grpc.unary_unary_rpc_method_handler(
          servicer.CreateRepo,
          request_deserializer=pfs__pb2.CreateRepoRequest.FromString,
          response_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
      ),
      'InspectRepo': grpc.unary_unary_rpc_method_handler(
          servicer.InspectRepo,
          request_deserializer=pfs__pb2.InspectRepoRequest.FromString,
          response_serializer=pfs__pb2.RepoInfo.SerializeToString,
      ),
      'ListRepo': grpc.unary_unary_rpc_method_handler(
          servicer.ListRepo,
          request_deserializer=pfs__pb2.ListRepoRequest.FromString,
          response_serializer=pfs__pb2.RepoInfos.SerializeToString,
      ),
      'DeleteRepo': grpc.unary_unary_rpc_method_handler(
          servicer.DeleteRepo,
          request_deserializer=pfs__pb2.DeleteRepoRequest.FromString,
          response_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
      ),
      'StartCommit': grpc.unary_unary_rpc_method_handler(
          servicer.StartCommit,
          request_deserializer=pfs__pb2.StartCommitRequest.FromString,
          response_serializer=pfs__pb2.Commit.SerializeToString,
      ),
      'FinishCommit': grpc.unary_unary_rpc_method_handler(
          servicer.FinishCommit,
          request_deserializer=pfs__pb2.FinishCommitRequest.FromString,
          response_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
      ),
      'InspectCommit': grpc.unary_unary_rpc_method_handler(
          servicer.InspectCommit,
          request_deserializer=pfs__pb2.InspectCommitRequest.FromString,
          response_serializer=pfs__pb2.CommitInfo.SerializeToString,
      ),
      'ListCommit': grpc.unary_unary_rpc_method_handler(
          servicer.ListCommit,
          request_deserializer=pfs__pb2.ListCommitRequest.FromString,
          response_serializer=pfs__pb2.CommitInfos.SerializeToString,
      ),
      'DeleteCommit': grpc.unary_unary_rpc_method_handler(
          servicer.DeleteCommit,
          request_deserializer=pfs__pb2.DeleteCommitRequest.FromString,
          response_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
      ),
      'FlushCommit': grpc.unary_stream_rpc_method_handler(
          servicer.FlushCommit,
          request_deserializer=pfs__pb2.FlushCommitRequest.FromString,
          response_serializer=pfs__pb2.CommitInfo.SerializeToString,
      ),
      'SubscribeCommit': grpc.unary_stream_rpc_method_handler(
          servicer.SubscribeCommit,
          request_deserializer=pfs__pb2.SubscribeCommitRequest.FromString,
          response_serializer=pfs__pb2.CommitInfo.SerializeToString,
      ),
      'BuildCommit': grpc.unary_unary_rpc_method_handler(
          servicer.BuildCommit,
          request_deserializer=pfs__pb2.BuildCommitRequest.FromString,
          response_serializer=pfs__pb2.Commit.SerializeToString,
      ),
      'ListBranch': grpc.unary_unary_rpc_method_handler(
          servicer.ListBranch,
          request_deserializer=pfs__pb2.ListBranchRequest.FromString,
          response_serializer=pfs__pb2.BranchInfos.SerializeToString,
      ),
      'SetBranch': grpc.unary_unary_rpc_method_handler(
          servicer.SetBranch,
          request_deserializer=pfs__pb2.SetBranchRequest.FromString,
          response_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
      ),
      'DeleteBranch': grpc.unary_unary_rpc_method_handler(
          servicer.DeleteBranch,
          request_deserializer=pfs__pb2.DeleteBranchRequest.FromString,
          response_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
      ),
      'PutFile': grpc.stream_unary_rpc_method_handler(
          servicer.PutFile,
          request_deserializer=pfs__pb2.PutFileRequest.FromString,
          response_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
      ),
      'GetFile': grpc.unary_stream_rpc_method_handler(
          servicer.GetFile,
          request_deserializer=pfs__pb2.GetFileRequest.FromString,
          response_serializer=google_dot_protobuf_dot_wrappers__pb2.BytesValue.SerializeToString,
      ),
      'InspectFile': grpc.unary_unary_rpc_method_handler(
          servicer.InspectFile,
          request_deserializer=pfs__pb2.InspectFileRequest.FromString,
          response_serializer=pfs__pb2.FileInfo.SerializeToString,
      ),
      'ListFile': grpc.unary_unary_rpc_method_handler(
          servicer.ListFile,
          request_deserializer=pfs__pb2.ListFileRequest.FromString,
          response_serializer=pfs__pb2.FileInfos.SerializeToString,
      ),
      'GlobFile': grpc.unary_unary_rpc_method_handler(
          servicer.GlobFile,
          request_deserializer=pfs__pb2.GlobFileRequest.FromString,
          response_serializer=pfs__pb2.FileInfos.SerializeToString,
      ),
      'DiffFile': grpc.unary_unary_rpc_method_handler(
          servicer.DiffFile,
          request_deserializer=pfs__pb2.DiffFileRequest.FromString,
          response_serializer=pfs__pb2.DiffFileResponse.SerializeToString,
      ),
      'DeleteFile': grpc.unary_unary_rpc_method_handler(
          servicer.DeleteFile,
          request_deserializer=pfs__pb2.DeleteFileRequest.FromString,
          response_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
      ),
      'DeleteAll': grpc.unary_unary_rpc_method_handler(
          servicer.DeleteAll,
          request_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
          response_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'pfs.API', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))


class ObjectAPIStub(object):
  # missing associated documentation comment in .proto file
  pass

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.PutObject = channel.stream_unary(
        '/pfs.ObjectAPI/PutObject',
        request_serializer=pfs__pb2.PutObjectRequest.SerializeToString,
        response_deserializer=pfs__pb2.Object.FromString,
        )
    self.GetObject = channel.unary_stream(
        '/pfs.ObjectAPI/GetObject',
        request_serializer=pfs__pb2.Object.SerializeToString,
        response_deserializer=google_dot_protobuf_dot_wrappers__pb2.BytesValue.FromString,
        )
    self.GetObjects = channel.unary_stream(
        '/pfs.ObjectAPI/GetObjects',
        request_serializer=pfs__pb2.GetObjectsRequest.SerializeToString,
        response_deserializer=google_dot_protobuf_dot_wrappers__pb2.BytesValue.FromString,
        )
    self.TagObject = channel.unary_unary(
        '/pfs.ObjectAPI/TagObject',
        request_serializer=pfs__pb2.TagObjectRequest.SerializeToString,
        response_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
        )
    self.InspectObject = channel.unary_unary(
        '/pfs.ObjectAPI/InspectObject',
        request_serializer=pfs__pb2.Object.SerializeToString,
        response_deserializer=pfs__pb2.ObjectInfo.FromString,
        )
    self.CheckObject = channel.unary_unary(
        '/pfs.ObjectAPI/CheckObject',
        request_serializer=pfs__pb2.CheckObjectRequest.SerializeToString,
        response_deserializer=pfs__pb2.CheckObjectResponse.FromString,
        )
    self.ListObjects = channel.unary_stream(
        '/pfs.ObjectAPI/ListObjects',
        request_serializer=pfs__pb2.ListObjectsRequest.SerializeToString,
        response_deserializer=pfs__pb2.Object.FromString,
        )
    self.DeleteObjects = channel.unary_unary(
        '/pfs.ObjectAPI/DeleteObjects',
        request_serializer=pfs__pb2.DeleteObjectsRequest.SerializeToString,
        response_deserializer=pfs__pb2.DeleteObjectsResponse.FromString,
        )
    self.GetTag = channel.unary_stream(
        '/pfs.ObjectAPI/GetTag',
        request_serializer=pfs__pb2.Tag.SerializeToString,
        response_deserializer=google_dot_protobuf_dot_wrappers__pb2.BytesValue.FromString,
        )
    self.InspectTag = channel.unary_unary(
        '/pfs.ObjectAPI/InspectTag',
        request_serializer=pfs__pb2.Tag.SerializeToString,
        response_deserializer=pfs__pb2.ObjectInfo.FromString,
        )
    self.ListTags = channel.unary_stream(
        '/pfs.ObjectAPI/ListTags',
        request_serializer=pfs__pb2.ListTagsRequest.SerializeToString,
        response_deserializer=pfs__pb2.ListTagsResponse.FromString,
        )
    self.DeleteTags = channel.unary_unary(
        '/pfs.ObjectAPI/DeleteTags',
        request_serializer=pfs__pb2.DeleteTagsRequest.SerializeToString,
        response_deserializer=pfs__pb2.DeleteTagsResponse.FromString,
        )
    self.Compact = channel.unary_unary(
        '/pfs.ObjectAPI/Compact',
        request_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
        response_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
        )


class ObjectAPIServicer(object):
  # missing associated documentation comment in .proto file
  pass

  def PutObject(self, request_iterator, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def GetObject(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def GetObjects(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def TagObject(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def InspectObject(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def CheckObject(self, request, context):
    """CheckObject checks if an object exists in the blob store without
    actually reading the object.
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def ListObjects(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def DeleteObjects(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def GetTag(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def InspectTag(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def ListTags(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def DeleteTags(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def Compact(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')


def add_ObjectAPIServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'PutObject': grpc.stream_unary_rpc_method_handler(
          servicer.PutObject,
          request_deserializer=pfs__pb2.PutObjectRequest.FromString,
          response_serializer=pfs__pb2.Object.SerializeToString,
      ),
      'GetObject': grpc.unary_stream_rpc_method_handler(
          servicer.GetObject,
          request_deserializer=pfs__pb2.Object.FromString,
          response_serializer=google_dot_protobuf_dot_wrappers__pb2.BytesValue.SerializeToString,
      ),
      'GetObjects': grpc.unary_stream_rpc_method_handler(
          servicer.GetObjects,
          request_deserializer=pfs__pb2.GetObjectsRequest.FromString,
          response_serializer=google_dot_protobuf_dot_wrappers__pb2.BytesValue.SerializeToString,
      ),
      'TagObject': grpc.unary_unary_rpc_method_handler(
          servicer.TagObject,
          request_deserializer=pfs__pb2.TagObjectRequest.FromString,
          response_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
      ),
      'InspectObject': grpc.unary_unary_rpc_method_handler(
          servicer.InspectObject,
          request_deserializer=pfs__pb2.Object.FromString,
          response_serializer=pfs__pb2.ObjectInfo.SerializeToString,
      ),
      'CheckObject': grpc.unary_unary_rpc_method_handler(
          servicer.CheckObject,
          request_deserializer=pfs__pb2.CheckObjectRequest.FromString,
          response_serializer=pfs__pb2.CheckObjectResponse.SerializeToString,
      ),
      'ListObjects': grpc.unary_stream_rpc_method_handler(
          servicer.ListObjects,
          request_deserializer=pfs__pb2.ListObjectsRequest.FromString,
          response_serializer=pfs__pb2.Object.SerializeToString,
      ),
      'DeleteObjects': grpc.unary_unary_rpc_method_handler(
          servicer.DeleteObjects,
          request_deserializer=pfs__pb2.DeleteObjectsRequest.FromString,
          response_serializer=pfs__pb2.DeleteObjectsResponse.SerializeToString,
      ),
      'GetTag': grpc.unary_stream_rpc_method_handler(
          servicer.GetTag,
          request_deserializer=pfs__pb2.Tag.FromString,
          response_serializer=google_dot_protobuf_dot_wrappers__pb2.BytesValue.SerializeToString,
      ),
      'InspectTag': grpc.unary_unary_rpc_method_handler(
          servicer.InspectTag,
          request_deserializer=pfs__pb2.Tag.FromString,
          response_serializer=pfs__pb2.ObjectInfo.SerializeToString,
      ),
      'ListTags': grpc.unary_stream_rpc_method_handler(
          servicer.ListTags,
          request_deserializer=pfs__pb2.ListTagsRequest.FromString,
          response_serializer=pfs__pb2.ListTagsResponse.SerializeToString,
      ),
      'DeleteTags': grpc.unary_unary_rpc_method_handler(
          servicer.DeleteTags,
          request_deserializer=pfs__pb2.DeleteTagsRequest.FromString,
          response_serializer=pfs__pb2.DeleteTagsResponse.SerializeToString,
      ),
      'Compact': grpc.unary_unary_rpc_method_handler(
          servicer.Compact,
          request_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
          response_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'pfs.ObjectAPI', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))
