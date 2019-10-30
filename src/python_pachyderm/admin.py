from python_pachyderm.proto.admin import admin_pb2 as admin_proto
from python_pachyderm.proto.admin import admin_pb2_grpc as admin_grpc
from python_pachyderm.proto.pps import pps_pb2 as pps_proto


class AdminMixin:
    @property
    def _admin_stub(self):
        if not hasattr(self, "__admin_stub"):
            self.__admin_stub = self._create_stub(admin_grpc)
        return self.__admin_stub

    def extract(self, url=None, no_objects=None, no_repos=None, no_pipelines=None):
        """
        Extracts cluster data for backup. Yields `Op` objects.

        Params:

        * `url`: An optional string specifying an object storage URL. If set,
          data will be extracted to this URL rather than returned.
        * `no_objects`: An optional bool. If true, will cause extract to omit
          objects (and tags.)
        * `no_repos`: An optional bool. If true, will cause extract to omit
          repos, commits and branches.
        * `no_pipelines`: An optional bool. If true, will cause extract to
          omit pipelines.
        """
        url = url or ""
        req = admin_proto.ExtractRequest(URL=url, no_objects=no_objects, no_repos=no_repos, no_pipelines=no_pipelines)
        return self._admin_stub.Extract(req, metadata=self.metadata)

    def extract_pipeline(self, pipeline_name):
        """
        Extracts a pipeline for backup. Returns an `Op` object.

        Params:

        * `pipeline_name`: A string representing a pipeline name to extract.
        """
        pipeline = pps_proto.Pipeline(name=pipeline_name) if pipeline_name is not None else None
        req = admin_proto.ExtractPipelineRequest(pipeline=pipeline)
        return self._admin_stub.ExtractPipeline(req, metadata=self.metadata)

    def restore(self, requests):
        """
        Restores a cluster.

        Params:

        * `requests`: A generator of `RestoreRequest` objects.
        """
        self._admin_stub.Restore(requests, metadata=self.metadata)

    def inspect_cluster(self):
        """
        Inspects a cluster. Returns a `ClusterInfo` object.
        """
        req = admin_grpc.google_dot_protobuf_dot_empty__pb2.Empty()
        return self._admin_stub.InspectCluster(req, metadata=self.metadata)
