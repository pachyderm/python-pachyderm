import json
import base64
from typing import Dict, Iterator, List, Union

from python_pachyderm.pfs import commit_from, Commit
from python_pachyderm.service import Service, pps_proto, pfs_proto
from google.protobuf import empty_pb2, duration_pb2


class PPSMixin:
    def inspect_job(
        self, pipeline_name: str, job_id: str, wait: bool = False, details: bool = False
    ) -> pps_proto.JobInfo:
        """
        Inspects a job with a given ID. Returns a `JobInfo`.

        Params:

        * `pipeline_name`: A string representing the pipeline name.
        * `job_id`: The ID of the job to inspect.
        * `wait`: If true, block until the job completes.
        * `details`: If true, include worker details.
        """
        return self._req(
            Service.PPS,
            "InspectJob",
            job=pps_proto.Job(
                pipeline=pps_proto.Pipeline(name=pipeline_name), id=job_id
            ),
            wait=wait,
            details=details,
        )

    def inspect_job_set(
        self, job_set_id: str, wait: bool = False, details: bool = False
    ) -> Iterator[pps_proto.JobInfo]:
        """
        Inspects a job set with a given ID. Yields `JobInfo` objects.

        Params:

        * `job_set_id`: The ID of the job set to inspect.
        * `wait`: If true, block until the job set completes.
        * `details`: If true, include worker details.
        """
        return self._req(
            Service.PPS,
            "InspectJobSet",
            job_set=pps_proto.JobSet(id=job_set_id),
            wait=wait,
            details=details,
        )

    def list_job(
        self,
        pipeline_name: str = None,
        input_commit: Union[tuple, dict, Commit, pfs_proto.Commit, List] = None,
        history: int = 0,
        details: bool = False,
        jqFilter: str = None,
    ) -> Iterator[pps_proto.JobInfo]:
        """
        Lists jobs. Yields `JobInfo` objects.

        Params:

        * `pipeline_name`: An optional string representing a pipeline name to
          filter on.
        * `input_commit`: An optional list of tuples, strings, or `Commit`
          objects representing input commits to filter on.
        * `output_commit`: An optional tuple, string, or `Commit` object
          representing an output commit to filter on.
        * `history`: An optional int that indicates to return jobs from
          historical versions of pipelines. Semantics are:
            * 0: Return jobs from the current version of the pipeline or
              pipelines.
            * 1: Return the above and jobs from the next most recent version
            * 2: etc.
            * -1: Return jobs from all historical versions.
        * `details`: An optional bool indicating whether the result should
          include all pipeline details in each `JobInfo`, or limited information
          including name and status, but excluding information in the pipeline
          spec. Leaving this `None` (or `False`) can make the call significantly
          faster in clusters with a large number of pipelines and jobs. Note
          that if `input_commit` is set, this field is coerced to `True`.
        * `jqFilter`: An optional string containing a `jq` filter that can
          restrict the list of jobs returned, for convenience
        """
        if isinstance(input_commit, list):
            input_commit = [commit_from(ic) for ic in input_commit]
        elif input_commit is not None:
            input_commit = [commit_from(input_commit)]

        return self._req(
            Service.PPS,
            "ListJob",
            pipeline=pps_proto.Pipeline(name=pipeline_name)
            if pipeline_name is not None
            else None,
            input_commit=input_commit,
            history=history,
            details=details,
            jqFilter=jqFilter,
        )

    def list_job_set(self, details: bool = False) -> Iterator[pps_proto.JobSetInfo]:
        """
        Lists all jobs. Yields `JobSetInfo` objects.

        Params:

        * `details`: An optional bool indicating whether the result should
          include all pipeline details, or limited information including name
          and status, but excluding information in the pipeline spec. Leaving
          this `None` (or `False`) can make the call significantly faster in
          clusters with a large number of pipelines and jobs.
        """
        return self._req(
            Service.PPS,
            "ListJobSet",
            details=details,
        )

    def delete_job(self, pipeline_name: str, job_id: str) -> None:
        """
        Deletes a job by its ID.

        Params:

        * `pipeline_name`: A string representing the pipeline name.
        * `job_id`: The ID of the job to delete.
        """
        self._req(
            Service.PPS,
            "DeleteJob",
            job=pps_proto.Job(
                pipeline=pps_proto.Pipeline(name=pipeline_name), id=job_id
            ),
        )

    def stop_job(self, pipeline_name: str, job_id: str, reason: str = None) -> None:
        """
        Stops a job given its pipeline_name and job_id.

        Params:
        * `pipeline_name`: A string representing the pipeline name.
        * `job_id`: The ID of the job to stop.
        * `reason`: a str specifying the reason for stopping the job.
        """
        self._req(
            Service.PPS,
            "StopJob",
            job=pps_proto.Job(
                pipeline=pps_proto.Pipeline(name=pipeline_name), id=job_id
            ),
            reason=reason,
        )

    def inspect_datum(
        self, pipeline_name: str, job_id: str, datum_id: str
    ) -> pps_proto.DatumInfo:
        """
        Inspects a datum. Returns a `DatumInfo` object.

        Params:

        * `pipeline_name`: A string representing the pipeline name.
        * `job_id`: The ID of the job.
        * `datum_id`: The ID of the datum.
        """
        return self._req(
            Service.PPS,
            "InspectDatum",
            datum=pps_proto.Datum(
                id=datum_id,
                job=pps_proto.Job(
                    pipeline=pps_proto.Pipeline(name=pipeline_name), id=job_id
                ),
            ),
        )

    def list_datum(
        self, pipeline_name: str, job_id: str = None, input: pps_proto.Input = None
    ) -> Iterator[pps_proto.DatumInfo]:
        """
        Lists datums. Yields `DatumInfo` objects.

        Params:

        * `pipeline_name`: A string representing the pipeline name.
        * `job_id`: An optional int specifying the ID of a job. Exactly one of
          `job_id` (real) or `input` (hypothetical) must be set.
        * `input`: an `Input` object. The datums listed are the ones that would
           be run if a pipeline was created with the provided input.
        """
        return self._req(
            Service.PPS,
            "ListDatum",
            job=pps_proto.Job(
                pipeline=pps_proto.Pipeline(name=pipeline_name), id=job_id
            ),
            input=input,
        )

    def restart_datum(
        self, pipeline_name: str, job_id: str, data_filters: List[str] = None
    ) -> None:
        """
        Restarts a datum.

        Params:

        * `pipeline_name`: A string representing the pipeline name.
        * `job_id`: The ID of the job.
        * `data_filters`: An optional iterable of strings.
        """
        self._req(
            Service.PPS,
            "RestartDatum",
            job=pps_proto.Job(
                pipeline=pps_proto.Pipeline(name=pipeline_name), id=job_id
            ),
            data_filters=data_filters,
        )

    def create_pipeline(
        self,
        pipeline_name: str,
        transform: pps_proto.Transform,
        parallelism_spec: pps_proto.ParallelismSpec = None,
        egress: pps_proto.Egress = None,
        reprocess_spec: str = None,
        update: bool = False,
        output_branch_name: str = None,
        s3_out: bool = False,
        resource_requests: pps_proto.ResourceSpec = None,
        resource_limits: pps_proto.ResourceSpec = None,
        sidecar_resource_limits: pps_proto.ResourceSpec = None,
        input: pps_proto.Input = None,
        description: str = None,
        reprocess: bool = False,
        service: pps_proto.Service = None,
        datum_set_spec: pps_proto.DatumSetSpec = None,
        datum_timeout: duration_pb2.Duration = None,
        job_timeout: duration_pb2.Duration = None,
        salt: str = None,
        datum_tries: int = 3,
        scheduling_spec: pps_proto.SchedulingSpec = None,
        pod_patch: str = None,
        spout: pps_proto.Spout = None,
        spec_commit: pfs_proto.Commit = None,
        metadata: pps_proto.Metadata = None,
        autoscaling: bool = False,
    ) -> None:
        """
        Creates a pipeline. For more info, please refer to the pipeline spec
        document:
        http://docs.pachyderm.io/en/latest/reference/pipeline_spec.html

        Params:

        * `pipeline_name`: A string representing the pipeline name.
        * `transform`: A `Transform` object.
        * `parallelism_spec`: An optional `ParallelismSpec` object.
        * `egress`: An optional `Egress` object.
        * `update`: An optional bool specifying whether this should behave as
        an upsert.
        * `output_branch`: An optional string representing the branch to output
        results on.
        * `resource_requests`: An optional `ResourceSpec` object.
        * `resource_limits`: An optional `ResourceSpec` object.
        * `input`: An optional `Input` object.
        * `description`: An optional string describing the pipeline.
        * `reprocess`: An optional bool. If true, pachyderm forces the pipeline
        to reprocess all datums. It only has meaning if `update` is `True`.
        * `service`: An optional `Service` object.
        * `chunk_spec`: An optional `ChunkSpec` object.
        * `datum_timeout`: An optional `Duration` object.
        * `job_timeout`: An optional `Duration` object.
        * `salt`: An optional string.
        * `datum_tries`: An optional int.
        * `scheduling_spec`: An optional `SchedulingSpec` object.
        * `pod_patch`: An optional string.
        * `spout`: An optional `Spout` object.
        * `spec_commit`: An optional `Commit` object.
        * `metadata`: An optional `Metadata` object.
        * `s3_out`: An optional bool specifying whether the output repo should
        be exposed as an s3 gateway bucket.
        * `sidecar_resource_limits`: An optional `ResourceSpec` setting
        * `reprocess_spec`: An optional string specifying how to handle
        already-processed data
        resource limits for the pipeline sidecar.
        """

        self._req(
            Service.PPS,
            "CreatePipeline",
            pipeline=pps_proto.Pipeline(name=pipeline_name),
            transform=transform,
            parallelism_spec=parallelism_spec,
            egress=egress,
            update=update,
            output_branch=output_branch_name,
            s3_out=s3_out,
            resource_requests=resource_requests,
            resource_limits=resource_limits,
            sidecar_resource_limits=sidecar_resource_limits,
            input=input,
            description=description,
            reprocess=reprocess,
            metadata=metadata,
            service=service,
            datum_set_spec=datum_set_spec,
            datum_timeout=datum_timeout,
            job_timeout=job_timeout,
            salt=salt,
            datum_tries=datum_tries,
            scheduling_spec=scheduling_spec,
            pod_patch=pod_patch,
            spout=spout,
            spec_commit=spec_commit,
            reprocess_spec=reprocess_spec,
            autoscaling=autoscaling,
        )

    def create_pipeline_from_request(
        self, req: pps_proto.CreatePipelineRequest
    ) -> None:
        """
        Creates a pipeline from a `CreatePipelineRequest` object. Usually this
        would be used in conjunction with `util.parse_json_pipeline_spec` or
        `util.parse_dict_pipeline_spec`. If you're in pure python and not
        working with a pipeline spec file, the sibling method
        `create_pipeline` is more ergonomic.

        Params:

        * `req`: A `CreatePipelineRequest` object.
        """
        self._req(Service.PPS, "CreatePipeline", req=req)

    def inspect_pipeline(
        self, pipeline_name: str, history: int = 0, details: bool = False
    ) -> Iterator[pps_proto.PipelineInfo]:
        """
        Inspects a pipeline. Returns a `PipelineInfo` object.

        Params:

        * `pipeline_name`: A string representing the pipeline name.
        * `details`: An optional bool that indicates to return details
        on the pipeline.
        """
        if history == 0:
            return iter(
                [
                    self._req(
                        Service.PPS,
                        "InspectPipeline",
                        pipeline=pps_proto.Pipeline(name=pipeline_name),
                        details=details,
                    )
                ]
            )
        else:
            # `InspectPipeline` doesn't support history, but `ListPipeline`
            # with a pipeline filter does, so we use that here
            return self._req(
                Service.PPS,
                "ListPipeline",
                pipeline=pps_proto.Pipeline(name=pipeline_name),
                history=history,
                details=details,
            )

    def list_pipeline(
        self, history=None, details=None, jqFilter=None
    ) -> Iterator[pps_proto.PipelineInfo]:
        """
        Lists pipelines. Yields `PipelineInfo` objects.

        Params:

        * `pipeline_name`: An optional string representing the pipeline name.
        * `history`: An optional int that indicates to return jobs from
          historical versions of pipelines. Semantics are:
            * 0: Return jobs from the current version of the pipeline or
              pipelines.
            * 1: Return the above and jobs from the next most recent version
            * 2: etc.
            * -1: Return jobs from all historical versions.
        * `details`: An optional bool that indicates to return details
        on the pipeline(s).
        * `jqFilter`: An optional string containing a `jq` filter that can
          restrict the list of jobs returned, for convenience
        """
        return self._req(
            Service.PPS,
            "ListPipeline",
            history=history,
            details=details,
            jqFilter=jqFilter,
        )

    def delete_pipeline(
        self, pipeline_name: str, force: bool = False, keep_repo: bool = False
    ) -> None:
        """
        Deletes a pipeline.

        Params:

        * `pipeline_name`: A string representing the pipeline name.
        * `force`: Whether to force delete.
        * `keep_repo`: Whether to keep the repo.
        """
        self._req(
            Service.PPS,
            "DeletePipeline",
            pipeline=pps_proto.Pipeline(name=pipeline_name),
            force=force,
            keep_repo=keep_repo,
        )

    def delete_all_pipelines(self) -> None:
        """
        Deletes all pipelines in pachyderm.
        """
        self._req(
            Service.PPS,
            "DeleteAll",
            req=empty_pb2.Empty(),
        )

    def start_pipeline(self, pipeline_name: str) -> None:
        """
        Starts a pipeline.

        Params:

        * `pipeline_name`: A string representing the pipeline name.
        """
        self._req(
            Service.PPS,
            "StartPipeline",
            pipeline=pps_proto.Pipeline(name=pipeline_name),
        )

    def stop_pipeline(self, pipeline_name: str) -> None:
        """
        Stops a pipeline.

        Params:

        * `pipeline_name`: A string representing the pipeline name.
        """
        self._req(
            Service.PPS, "StopPipeline", pipeline=pps_proto.Pipeline(name=pipeline_name)
        )

    def run_cron(self, pipeline_name: str) -> None:
        """
        Explicitly triggers a pipeline with one or more cron inputs to run
        now.

        Params:

        * `pipeline_name`: A string representing the pipeline name.
        """

        self._req(
            Service.PPS,
            "RunCron",
            pipeline=pps_proto.Pipeline(name=pipeline_name),
        )

    def create_secret(
        self,
        secret_name: str,
        data: Dict[str, Union[str, bytes]],
        labels: Dict[str, str] = None,
        annotations: Dict[str, str] = None,
    ) -> None:
        """
        Creates a new secret.

        Params:

        * `secret_name`: The name of the secret to create.
        * `data`: A dict of string keys -> string or bytestring values to
        store in the secret. Each key must consist of alphanumeric characters,
        `-`, `_` or `.`.
        * `labels`: A dict of string keys -> string values representing the
        kubernetes labels to attach to the secret.
        * `annotations`: A dict representing the kubernetes annotations to
        attach to the secret.
        """

        encoded_data = {}
        for k, v in data.items():
            if isinstance(v, str):
                v = v.encode("utf8")
            encoded_data[k] = base64.b64encode(v).decode("utf8")

        f = json.dumps(
            {
                "kind": "Secret",
                "apiVersion": "v1",
                "metadata": {
                    "name": secret_name,
                    "labels": labels,
                    "annotations": annotations,
                },
                "data": encoded_data,
            }
        ).encode("utf8")

        self._req(Service.PPS, "CreateSecret", file=f)

    def delete_secret(self, secret_name: str) -> None:
        """
        Deletes a new secret.

        Params:

        * `secret_name`: The name of the secret to delete.
        """
        secret = pps_proto.Secret(name=secret_name)
        return self._req(Service.PPS, "DeleteSecret", secret=secret)

    def list_secret(self) -> List[pps_proto.SecretInfo]:
        """
        Lists secrets. Returns a list of `SecretInfo` objects.
        """

        return self._req(
            Service.PPS,
            "ListSecret",
            req=empty_pb2.Empty(),
        ).secret_info

    def inspect_secret(self, secret_name: str) -> pps_proto.SecretInfo:
        """
        Inspects a secret.

        Params:

        * `secret_name`: The name of the secret to inspect.
        """
        secret = pps_proto.Secret(name=secret_name)
        return self._req(Service.PPS, "InspectSecret", secret=secret)

    def get_pipeline_logs(
        self,
        pipeline_name: str,
        data_filters: List[str] = None,
        master: bool = False,
        datum: pps_proto.Datum = None,
        follow: bool = False,
        tail: int = 0,
        use_loki_backend: bool = False,
        since: duration_pb2.Duration = None,
    ) -> Iterator[pps_proto.LogMessage]:
        """
        Gets logs for a pipeline. Yields `LogMessage` objects.

        Params:

        * `pipeline_name`: A string representing a pipeline to get
          logs of.
        * `data_filters`: An optional iterable of strings specifying the names
          of input files from which we want processing logs. This may contain
          multiple files, to query pipelines that contain multiple inputs. Each
          filter may be an absolute path of a file within a pps repo, or it may
          be a hash for that file (to search for files at specific versions.)
        * `master`: An optional bool.
        * `datum`: An optional `Datum` object.
        * `follow`: An optional bool specifying whether logs should continue to
          stream forever.
        * `tail`: An optional int. If nonzero, the number of lines from the end
          of the logs to return.  Note: tail applies per container, so you will
          get tail * <number of pods> total lines back.
        * `use_loki_backend`: Whether to use loki as a backend for fetching
          logs. Requires a loki-enabled cluster.
        * `since`: An optional `Duration` object specifying the start time for
          returned logs
        """
        return self._req(
            Service.PPS,
            "GetLogs",
            pipeline=pps_proto.Pipeline(name=pipeline_name),
            data_filters=data_filters,
            master=master,
            datum=datum,
            follow=follow,
            tail=tail,
            use_loki_backend=use_loki_backend,
            since=since,
        )

    def get_job_logs(
        self,
        pipeline_name: str,
        job_id: str,
        data_filters: List[str] = None,
        datum: pps_proto.Datum = None,
        follow: bool = False,
        tail: int = 0,
        use_loki_backend: bool = False,
        since: duration_pb2.Duration = None,
    ) -> Iterator[pps_proto.LogMessage]:
        """
        Gets logs for a job. Yields `LogMessage` objects.

        Params:

        * `pipeline_name`: A string representing a pipeline.
        * `job_id`: A string representing a job to get logs of.
        * `data_filters`: An optional iterable of strings specifying the names
          of input files from which we want processing logs. This may contain
          multiple files, to query pipelines that contain multiple inputs. Each
          filter may be an absolute path of a file within a pps repo, or it may
          be a hash for that file (to search for files at specific versions.)
        * `datum`: An optional `Datum` object.
        * `follow`: An optional bool specifying whether logs should continue to
          stream forever.
        * `tail`: An optional int. If nonzero, the number of lines from the end
          of the logs to return.  Note: tail applies per container, so you will
          get tail * <number of pods> total lines back.
        * `use_loki_backend`: Whether to use loki as a backend for fetching
          logs. Requires a loki-enabled cluster.
        * `since`: An optional `Duration` object specifying the start time for
          returned logs
        """
        return self._req(
            Service.PPS,
            "GetLogs",
            job=pps_proto.Job(
                pipeline=pps_proto.Pipeline(name=pipeline_name), id=job_id
            ),
            data_filters=data_filters,
            datum=datum,
            follow=follow,
            tail=tail,
            use_loki_backend=use_loki_backend,
            since=since,
        )
