from python_pachyderm.proto.admin import admin_pb2_grpc as admin_grpc
from python_pachyderm.proto.pps import pps_pb2 as pps_proto
from python_pachyderm.service import Service


class AdminMixin:
    def extract(
        self,
        url=None,
        no_objects=None,
        no_repos=None,
        no_pipelines=None,
        no_auth=None,
        no_enterprise=None,
    ):
        """Extracts cluster data for backup. Yields `Op` objects.

        Parameters
        ----------
        url : str, optional
            A string specifying an object storage URL. If set, data will be
            extracted to this URL rather than returned.
        no_objects : bool, optional
            If true, will cause extract to omit objects (and tags.)
        no_repos : bool, optional
            If true, will cause extract to omit repos, commits and branches.
        no_pipelines : bool, optional
            If true, will cause extract to omit pipelines.
        no_auth : bool, optional
            If true, will cause extract to omit acls, tokens, etc.
        no_enterprise : bool, optional
            If true, will cause extract to omit any enterprise activation key
            (which may break auth restore)
        """
        return self._req(
            Service.ADMIN,
            "Extract",
            URL=url or "",
            no_objects=no_objects,
            no_repos=no_repos,
            no_pipelines=no_pipelines,
            no_auth=no_auth,
            no_enterprise=no_enterprise,
        )

    def extract_pipeline(self, pipeline_name):
        """Extracts a pipeline for backup. Returns an `Op` object.

        Parameters
        ----------
        pipeline_name : str
            The pipeline name to extract.
        """
        return self._req(
            Service.ADMIN,
            "ExtractPipeline",
            pipeline=pps_proto.Pipeline(name=pipeline_name)
            if pipeline_name is not None
            else None,
        )

    def restore(self, requests):
        """Restores a cluster.

        Parameters
        ----------
        requests : Iterator[RestoreRequest protobufs]
            A generator of `RestoreRequest` objects.
        """
        return self._req(Service.ADMIN, "Restore", req=requests)

    def inspect_cluster(self):
        """Inspects a cluster. Returns a `ClusterInfo` object."""
        return self._req(
            Service.ADMIN,
            "InspectCluster",
            req=admin_grpc.google_dot_protobuf_dot_empty__pb2.Empty(),
        )
