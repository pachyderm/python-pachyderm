import json
import base64
from typing import Dict, Iterator, List, Union

import grpc
from google.protobuf import empty_pb2, duration_pb2

from python_pachyderm.pfs import commit_from, SubcommitType
from python_pachyderm.proto.v2.pfs import pfs_pb2
from python_pachyderm.proto.v2.pps import pps_pb2, pps_pb2_grpc


class PPSMixin:
    """A mixin for pps-related functionality."""

    _channel: grpc.Channel

    def __init__(self):
        self.__stub = pps_pb2_grpc.APIStub(self._channel)
        super().__init__()

    def inspect_job(
        self,
        job_id: str,
        pipeline_name: str = None,
        wait: bool = False,
        details: bool = False,
    ) -> Iterator[pps_pb2.JobInfo]:
        """Inspects a job.

        Parameters
        ----------
        job_id : str
            The ID of the job.
        pipeline_name : str, optional
            The name of a pipeline.
        wait : bool, optional
            If true, wait until the job completes.
        details : bool, optional
            If true, return worker details.

        Returns
        -------
        Iterator[pps_pb2.JobInfo]
            An iterator of protobuf objects that contain info on a subjob
            (jobs at the pipeline-level).

        Examples
        --------
        >>> # Look at all subjobs in a job
        >>> subjobs = list(client.inspect_job("467c580611234cdb8cc9758c7aa96087"))
        ...
        >>> # Look at single subjob (job at the pipeline-level)
        >>> subjob = list(client.inspect_job("467c580611234cdb8cc9758c7aa96087", "foo"))[0]

        .. # noqa: W505
        """
        if pipeline_name is not None:
            message = pps_pb2.InspectJobRequest(
                details=details,
                job=pps_pb2.Job(
                    pipeline=pps_pb2.Pipeline(name=pipeline_name), id=job_id
                ),
                wait=wait,
            )
            return iter([self.__stub.InspectJob(message)])
        else:
            message = pps_pb2.InspectJobSetRequest(
                details=details,
                job_set=pps_pb2.JobSet(id=job_id),
                wait=wait,
            )
            return self.__stub.InspectJobSet(message)

    def list_job(
        self,
        pipeline_name: str = None,
        input_commit: SubcommitType = None,
        history: int = 0,
        details: bool = False,
        jqFilter: str = None,
    ) -> Union[Iterator[pps_pb2.JobInfo], Iterator[pps_pb2.JobSetInfo]]:
        """Lists jobs.

        Parameters
        ----------
        pipeline_name : str, optional
            The name of a pipeline. If set, returns subjobs (job at the
            pipeline-level) only from this pipeline.
        input_commit : SubcommitType, optional
            A commit or list of commits from the input repo to filter jobs on.
            Only impacts returned results if `pipeline_name` is specified.
        history : int, optional
            Indicates to return jobs from historical versions of
            `pipeline_name`. Semantics are:

            - 0: Return jobs from the current version of `pipeline_name`
            - 1: Return the above and jobs from the next most recent version
            - 2: etc.
            - -1: Return jobs from all historical versions of `pipeline_name`

        details : bool, optional
            If true, return pipeline details for `pipeline_name`. Leaving this
            ``None`` (or ``False``) can make the call significantly faster in
            clusters with a large number of pipelines and jobs. Note that if
            `input_commit` is valid, this field is coerced to `True`.
        jqFilter : str, optional
            A ``jq`` filter that can filter the list of jobs returned, only if
            `pipeline_name` is provided.

        Returns
        -------
        Union[Iterator[pps_pb2.JobInfo], Iterator[pps_pb2.JobSetInfo]]
            An iterator of protobuf objects that either contain info on a
            subjob (job at the pipeline-level), if `pipeline_name` was
            specified, or a job, if `pipeline_name` wasn't specified.

        Examples
        --------
        >>> # List all jobs
        >>> jobs = list(client.list_job())
        ...
        >>> # List all jobs at a pipeline-level
        >>> subjobs = list(client.list_job("foo"))

        .. # noqa: W505
        """
        if pipeline_name is not None:
            if isinstance(input_commit, list):
                input_commit = [commit_from(ic) for ic in input_commit]
            elif input_commit is not None:
                input_commit = [commit_from(input_commit)]
            message = pps_pb2.ListJobRequest(
                details=details,
                history=history,
                input_commit=input_commit,
                jqFilter=jqFilter,
                pipeline=pps_pb2.Pipeline(name=pipeline_name),
            )
            return self.__stub.ListJob(message)
        else:
            message = pps_pb2.ListJobSetRequest(details=details)
            return self.__stub.ListJobSet(message)

    def delete_job(self, job_id: str, pipeline_name: str) -> None:
        """Deletes a subjob (job at the pipeline-level).

        Parameters
        ----------
        job_id : str
            The ID of the job.
        pipeline_name : str
            The name of the pipeline.
        """
        message = pps_pb2.DeleteJobRequest(
            job=pps_pb2.Job(
                id=job_id,
                pipeline=pps_pb2.Pipeline(name=pipeline_name),
            )
        )
        self.__stub.DeleteJob(message)

    def stop_job(self, job_id: str, pipeline_name: str, reason: str = None) -> None:
        """Stops a subjob (job at the pipeline-level).

        Parameters
        ----------
        job_id : str
            The ID of the job.
        pipeline_name : str
            The name of the pipeline.
        reason : str, optional
            A reason for stopping the job.
        """
        message = pps_pb2.StopJobRequest(
            job=pps_pb2.Job(
                id=job_id,
                pipeline=pps_pb2.Pipeline(name=pipeline_name),
            ),
            reason=reason,
        )
        self.__stub.StopJob(message)

    def inspect_datum(
        self, pipeline_name: str, job_id: str, datum_id: str
    ) -> pps_pb2.DatumInfo:
        """Inspects a datum.

        Parameters
        ----------
        pipeline_name : str
            The name of the pipeline.
        job_id : str
            The ID of the job.
        datum_id : str
            The ID of the datum.

        Returns
        -------
        pps_pb2.DatumInfo
            A protobuf object with info on the datum.
        """
        message = pps_pb2.InspectDatumRequest(
            datum=pps_pb2.Datum(
                id=datum_id,
                job=pps_pb2.Job(
                    id=job_id,
                    pipeline=pps_pb2.Pipeline(name=pipeline_name),
                ),
            ),
        )
        return self.__stub.InspectDatum(message)

    def list_datum(
        self,
        pipeline_name: str = None,
        job_id: str = None,
        input: pps_pb2.Input = None,
    ) -> Iterator[pps_pb2.DatumInfo]:
        """Lists datums. Exactly one of (`pipeline_name`, `job_id`) (real) or
        `input` (hypothetical) must be set.

        Parameters
        ----------
        pipeline_name : str, optional
            The name of the pipeline.
        job_id : str, optional
            The ID of a job.
        input : pps_pb2.Input, optional
            A protobuf object that filters the datums returned. The datums
            listed are ones that would be run if a pipeline was created with
            the provided input.

        Returns
        -------
        Iterator[pps_pb2.DatumInfo]
            An iterator of protobuf objects that contain info on a datum.

        Examples
        --------
        >>> # See hypothetical datums with specified input cross
        >>> datums = list(client.list_datum(input=pps_pb2.Input(
        ...     pfs=pps_pb2.PFSInput(repo="foo", branch="master", glob="/*"),
        ...     cross=[
        ...         pps_pb2.Input(pfs=pps_pb2.PFSInput(repo="bar", branch="master", glob="/")),
        ...         pps_pb2.Input(pfs=pps_pb2.PFSInput(repo="baz", branch="master", glob="/*/*")),
        ...     ]
        ... )))

        .. # noqa: W505
        """
        message = pps_pb2.ListDatumRequest()
        if pipeline_name is not None and job_id is not None:
            message.job.CopyFrom(
                pps_pb2.Job(pipeline=pps_pb2.Pipeline(name=pipeline_name), id=job_id)
            )
        else:
            message.input.CopyFrom(input)
        return self.__stub.ListDatum(message)

    def restart_datum(
        self, pipeline_name: str, job_id: str, data_filters: List[str] = None
    ) -> None:
        """Restarts a datum.

        Parameters
        ----------
        pipeline_name : str
            The name of the pipeline.
        job_id : str
            The ID of the job.
        data_filters : List[str], optional
            A list of paths or hashes of datums that filter which datums are
            restarted.
        """
        message = pps_pb2.RestartDatumRequest(
            data_filters=data_filters,
            job=pps_pb2.Job(pipeline=pps_pb2.Pipeline(name=pipeline_name), id=job_id),
        )
        self.__stub.RestartDatum(message)

    def create_pipeline(
        self,
        pipeline_name: str,
        transform: pps_pb2.Transform,
        parallelism_spec: pps_pb2.ParallelismSpec = None,
        egress: pps_pb2.Egress = None,
        reprocess_spec: str = None,
        update: bool = False,
        output_branch_name: str = None,
        s3_out: bool = False,
        resource_requests: pps_pb2.ResourceSpec = None,
        resource_limits: pps_pb2.ResourceSpec = None,
        sidecar_resource_limits: pps_pb2.ResourceSpec = None,
        input: pps_pb2.Input = None,
        description: str = None,
        reprocess: bool = False,
        service: pps_pb2.Service = None,
        datum_set_spec: pps_pb2.DatumSetSpec = None,
        datum_timeout: duration_pb2.Duration = None,
        job_timeout: duration_pb2.Duration = None,
        salt: str = None,
        datum_tries: int = 3,
        scheduling_spec: pps_pb2.SchedulingSpec = None,
        pod_patch: str = None,
        spout: pps_pb2.Spout = None,
        spec_commit: pfs_pb2.Commit = None,
        metadata: pps_pb2.Metadata = None,
        autoscaling: bool = False,
    ) -> None:
        """Creates a pipeline.

        For info on the params, please refer to the pipeline spec document:
        http://docs.pachyderm.io/en/latest/reference/pipeline_spec.html

        Parameters
        ----------
        pipeline_name : str
            The pipeline name.
        transform : pps_pb2.Transform
            The image and commands run during pipeline execution.
        parallelism_spec : pps_pb2.ParallelismSpec, optional
            Specifies how the pipeline is parallelized.
        egress : pps_pb2.Egress, optional
            An external data store to publish the results of the pipeline to.
        reprocess_spec : str, optional
            Specifies how to handle already-processed datums.
        update : bool, optional
            If true, updates the existing pipeline with new args.
        output_branch_name : str, optional
            The branch name to output results on.
        s3_out : bool, optional
            If true, the output repo is exposed as an S3 gateway bucket.
        resource_requests : pps_pb2.ResourceSpec, optional
            The amount of resources that the pipeline workers will consume.
        resource_limits: pps_pb2.ResourceSpec, optional
            The upper threshold of allowed resources a given worker can
            consume. If a worker exceeds this value, it will be evicted.
        sidecar_resource_limits : pps_pb2.ResourceSpec, optional
            The upper threshold of resources allocated to the sidecar
            containers.
        input : pps_pb2.Input, optional
            The input repos to the pipeline. Commits to these repos will
            automatically trigger the pipeline to create new jobs to
            process them.
        description : str, optional
            A description of the pipeline.
        reprocess : bool, optional
            If true, forces the pipeline to reprocess all datums. Only has
            meaning if `update` is ``True``.
        service : pps_pb2.Service, optional
            Creates a Service pipeline instead of a normal pipeline.
        datum_set_spec : pps_pb2.DatumSetSpec, optional
            Specifies how a pipeline should split its datums into datum sets.
        datum_timeout : duration_pb2.Duration, optional
            The maximum execution time allowed for each datum.
        job_timeout : duration_pb2.Duration, optional
            The maximum execution time allowed for a job.
        salt : str, optional
            A tag for the pipeline.
        datum_tries : int, optional
            The number of times a job attempts to run on a datum when a failure
            occurs.
        scheduling_spec : pps_pb2.SchedulingSpec, optional
            Specifies how the pods for a pipeline should be scheduled.
        pod_patch : str, optional
            Allows one to set fields in the pod spec that haven't been
            explicitly exposed in the rest of the pipeline spec.
        spout : pps_pb2.Spout, optional
            Creates a Spout pipeline instead of a normal pipeline.
        spec_commit : pfs_pb2.Commit, optional
            A spec commit to base the pipeline spec from.
        metadata : pps_pb2.Metadata, optional
            Kubernetes labels and annotations to add as metadata to the
            pipeline pods.
        autoscaling : bool, optional
            If true, automatically scales the worker pool based on the datums
            it has to process.

        Notes
        -----
        If creating a Spout pipeline, when committing data to the repo, use
        commit methods (``client.commit()``, ``client.start_commit()``, etc.)
        or :class:`.ModifyFileClient` methods (``mfc.put_file_from_bytes``,
        ``mfc.delete_file()``, etc.)

        For other pipelines, when committing data to the repo, write out to
        ``/pfs/out/``.

        Examples
        --------
        >>> client.create_pipeline(
        ...     "foo",
        ...     transform=pps_pb2.Transform(
        ...         cmd=["python3", "main.py"],
        ...         image="example/image",
        ...     ),
        ...     input=pps_pb2.Input(pfs=pps_pb2.PFSInput(
        ...         repo="foo",
        ...         branch="master",
        ...         glob="/*"
        ...     ))
        ... )
        """
        message = pps_pb2.CreatePipelineRequest(
            pipeline=pps_pb2.Pipeline(name=pipeline_name),
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
        self.__stub.CreatePipeline(message)

    def create_pipeline_from_request(self, req: pps_pb2.CreatePipelineRequest) -> None:
        """Creates a pipeline from a ``CreatePipelineRequest`` object. Usually
        used in conjunction with ``util.parse_json_pipeline_spec()`` or
        ``util.parse_dict_pipeline_spec()``.

        Parameters
        ----------
        req : pps_pb2.CreatePipelineRequest
            The ``CreatePipelineRequest`` object.
        """
        self.__stub.CreatePipeline(req)

    def inspect_pipeline(
        self, pipeline_name: str, history: int = 0, details: bool = False
    ) -> Iterator[pps_pb2.PipelineInfo]:
        """.. # noqa: W505

        Inspects a pipeline.

        Parameters
        ----------
        pipeline_name : str
            The name of the pipeline.
        history : int, optional
            Indicates to return historical versions of `pipeline_name`.
            Semantics are:

            - 0: Return current version of `pipeline_name`
            - 1: Return the above and `pipeline_name` from the next most recent version.
            - 2: etc.
            - -1: Return all historical versions of `pipeline_name`.

        details : bool, optional
            If true, return pipeline details.

        Returns
        -------
        Iterator[pps_pb2.PipelineInfo]
            An iterator of protobuf objects that contain info on a pipeline.

        Examples
        --------
        >>> pipeline = next(client.inspect_pipeline("foo"))
        ...
        >>> for p in client.inspect_pipeline("foo", 2):
        >>>     print(p)
        """
        if history == 0:
            message = pps_pb2.InspectPipelineRequest(
                details=details,
                pipeline=pps_pb2.Pipeline(name=pipeline_name),
            )
            return iter([self.__stub.InspectPipeline(message)])
        else:
            # `InspectPipeline` doesn't support history, but `ListPipeline`
            # with a pipeline filter does, so we use that here
            message = pps_pb2.ListPipelineRequest(
                details=details,
                history=history,
                pipeline=pps_pb2.Pipeline(name=pipeline_name),
            )
            return self.__stub.ListPipeline(message)

    def list_pipeline(
        self, history: int = 0, details: bool = False, jqFilter: str = None
    ) -> Iterator[pps_pb2.PipelineInfo]:
        """.. # noqa: W505

        Lists pipelines.

        Parameters
        ----------
        history : int, optional
            Indicates to return historical versions of `pipeline_name`.
            Semantics are:

            - 0: Return current version of `pipeline_name`
            - 1: Return the above and `pipeline_name` from the next most recent version.
            - 2: etc.
            - -1: Return all historical versions of `pipeline_name`.

        details : bool, optional
            If true, return pipeline details.
        jqFilter : str, optional
            A ``jq`` filter that can filter the list of pipelines returned.

        Returns
        -------
        Iterator[pps_pb2.PipelineInfo]
            An iterator of protobuf objects that contain info on a pipeline.

        Examples
        --------
        >>> pipelines = list(client.list_pipeline())
        """
        message = pps_pb2.ListPipelineRequest(
            details=details,
            history=history,
            jqFilter=jqFilter,
        )
        return self.__stub.ListPipeline(message)

    def delete_pipeline(
        self, pipeline_name: str, force: bool = False, keep_repo: bool = False
    ) -> None:
        """Deletes a pipeline.

        Parameters
        ----------
        pipeline_name : str
            The name of the pipeline.
        force : bool, optional
            If true, forces the pipeline deletion.
        keep_repo : bool, optional
            If true, keeps the output repo.
        """
        message = pps_pb2.DeletePipelineRequest(
            force=force,
            keep_repo=keep_repo,
            pipeline=pps_pb2.Pipeline(name=pipeline_name),
        )
        self.__stub.DeletePipeline(message)

    def delete_all_pipelines(self) -> None:
        """Deletes all pipelines."""
        message = empty_pb2.Empty()
        self.__stub.DeleteAll(message)

    def start_pipeline(self, pipeline_name: str) -> None:
        """Starts a pipeline.

        Parameters
        ----------
        pipeline_name : str
            The name of the pipeline.
        """
        message = pps_pb2.StartPipelineRequest(
            pipeline=pps_pb2.Pipeline(name=pipeline_name),
        )
        self.__stub.StartPipeline(message)

    def stop_pipeline(self, pipeline_name: str) -> None:
        """Stops a pipeline.

        Parameters
        ----------
        pipeline_name : str
            The name of the pipeline.
        """
        message = pps_pb2.StopPipelineRequest(
            pipeline=pps_pb2.Pipeline(name=pipeline_name)
        )
        self.__stub.StopPipeline(message)

    def run_cron(self, pipeline_name: str) -> None:
        """Triggers a cron pipeline to run now.

        For more info on cron pipelines:
        https://docs.pachyderm.com/latest/concepts/pipeline-concepts/pipeline/cron/

        Parameters
        ----------
        pipeline_name : str
            The name of the pipeline.
        """
        message = pps_pb2.RunCronRequest(
            pipeline=pps_pb2.Pipeline(name=pipeline_name),
        )
        self.__stub.RunCron(message)

    def create_secret(
        self,
        secret_name: str,
        data: Dict[str, Union[str, bytes]],
        labels: Dict[str, str] = None,
        annotations: Dict[str, str] = None,
    ) -> None:
        """Creates a new secret.

        Parameters
        ----------
        secret_name : str
            The name of the secret.
        data : Dict[str, Union[str, bytes]]
            The data to store in the secret. Each key must consist of
            alphanumeric characters ``-``, ``_`` or ``.``.
        labels : Dict[str, str], optional
            Kubernetes labels to attach to the secret.
        annotations : Dict[str, str], optional
            Kubernetes annotations to attach to the secret.
        """

        encoded_data = {}
        for k, v in data.items():
            if isinstance(v, str):
                v = v.encode("utf8")
            encoded_data[k] = base64.b64encode(v).decode("utf8")

        file = json.dumps(
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

        message = pps_pb2.CreateSecretRequest(file=file)
        self.__stub.CreateSecret(message)

    def delete_secret(self, secret_name: str) -> None:
        """Deletes a secret.

        Parameters
        ----------
        secret_name : str
            The name of the secret.
        """
        message = pps_pb2.DeleteSecretRequest(
            secret=pps_pb2.Secret(name=secret_name),
        )
        self.__stub.DeleteSecret(message)

    def list_secret(self) -> List[pps_pb2.SecretInfo]:
        """Lists secrets.

        Returns
        -------
        List[pps_pb2.SecretInfo]
            A list of protobuf objects that contain info on a secret.
        """
        message = empty_pb2.Empty()
        return self.__stub.ListSecret(message).secret_info

    def inspect_secret(self, secret_name: str) -> pps_pb2.SecretInfo:
        """Inspects a secret.

        Parameters
        ----------
        secret_name : str
            The name of the secret.

        Returns
        -------
        pps_pb2.SecretInfo
            A protobuf object with info on the secret.
        """
        message = pps_pb2.InspectSecretRequest(
            secret=pps_pb2.Secret(name=secret_name),
        )
        return self.__stub.InspectSecret(message)

    def get_pipeline_logs(
        self,
        pipeline_name: str,
        data_filters: List[str] = None,
        master: bool = False,
        datum: pps_pb2.Datum = None,
        follow: bool = False,
        tail: int = 0,
        use_loki_backend: bool = False,
        since: duration_pb2.Duration = None,
    ) -> Iterator[pps_pb2.LogMessage]:
        """Gets logs for a pipeline.

        Parameters
        ----------
        pipeline_name : str
            The name of the pipeline.
        data_filters : List[str], optional
            A list of the names of input files from which we want processing
            logs. This may contain multiple files, in case `pipeline_name`
            contains multiple inputs. Each filter may be an absolute path of a
            file within a repo, or it may be a hash for that file (to search
            for files at specific versions).
        master : bool, optional
            If true, includes logs from the master
        datum : pps_pb2.Datum, optional
            Filters log lines for the specified datum.
        follow : bool, optional
            If true, continue to follow new logs as they appear.
        tail : int, optional
            If nonzero, the number of lines from the end of the logs to return.
            Note: tail applies per container, so you will get
            `tail` * <number of pods> total lines back.
        use_loki_backend : bool, optional
            If true, use loki as a backend, rather than Kubernetes, for
            fetching logs. Requires a loki-enabled cluster.
        since : duration_pb2.Duration, optional
            Specifies how far in the past to return logs from.

        Returns
        -------
        Iterator[pps_pb2.LogMessage]
            An iterator of protobuf objects that contain info on a log from a
            PPS worker. If `follow` is set to ``True``, use ``next()`` to
            iterate through as the returned stream is potentially endless.
            Might block your code otherwise.
        """
        message = pps_pb2.GetLogsRequest(
            pipeline=pps_pb2.Pipeline(name=pipeline_name),
            data_filters=data_filters,
            master=master,
            datum=datum,
            follow=follow,
            tail=tail,
            use_loki_backend=use_loki_backend,
            since=since,
        )
        return self.__stub.GetLogs(message)

    def get_job_logs(
        self,
        pipeline_name: str,
        job_id: str,
        data_filters: List[str] = None,
        datum: pps_pb2.Datum = None,
        follow: bool = False,
        tail: int = 0,
        use_loki_backend: bool = False,
        since: duration_pb2.Duration = None,
    ) -> Iterator[pps_pb2.LogMessage]:
        """Gets logs for a job.

        Parameters
        ----------
        pipeline_name : str
            The name of the pipeline.
        job_id : str
            The ID of the job.
        data_filters : List[str], optional
            A list of the names of input files from which we want processing
            logs. This may contain multiple files, in case `pipeline_name`
            contains multiple inputs. Each filter may be an absolute path of a
            file within a repo, or it may be a hash for that file (to search
            for files at specific versions).
        datum : pps_pb2.Datum, optional
            Filters log lines for the specified datum.
        follow : bool, optional
            If true, continue to follow new logs as they appear.
        tail : int, optional
            If nonzero, the number of lines from the end of the logs to return.
            Note: tail applies per container, so you will get
            `tail` * <number of pods> total lines back.
        use_loki_backend : bool, optional
            If true, use loki as a backend, rather than Kubernetes, for
            fetching logs. Requires a loki-enabled cluster.
        since : duration_pb2.Duration, optional
            Specifies how far in the past to return logs from.

        Returns
        -------
        Iterator[pps_pb2.LogMessage]
            An iterator of protobuf objects that contain info on a log from a
            PPS worker. If `follow` is set to ``True``, use ``next()`` to
            iterate through as the returned stream is potentially endless.
            Might block your code otherwise.
        """
        message = pps_pb2.GetLogsRequest(
            job=pps_pb2.Job(pipeline=pps_pb2.Pipeline(name=pipeline_name), id=job_id),
            data_filters=data_filters,
            datum=datum,
            follow=follow,
            tail=tail,
            use_loki_backend=use_loki_backend,
            since=since,
        )
        return self.__stub.GetLogs(message)
