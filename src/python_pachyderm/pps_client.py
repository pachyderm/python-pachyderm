# -*- coding: utf-8 -*-

from python_pachyderm.client.pps import pps_pb2 as proto
from python_pachyderm.client.pps import pps_pb2_grpc as grpc
from python_pachyderm.util import commit_from, get_address, get_metadata


class PpsClient(object):
    def __init__(self, host=None, port=None, auth_token=None):
        """
        Creates a client to connect to PPS.

        host: The pachd host. Default is 'localhost'.
        port: The port to connect to. Default is 30650.
        auth_token: The authentication token; used if authentication is
        enabled on the cluster. Default to `None`.
        """

        address = get_address(host, port)
        self.metadata = get_metadata(auth_token)
        self.channel = grpc.grpc.insecure_channel(address)
        self.stub = grpc.APIStub(self.channel)

    def inspect_job(self, job_id, block_state=None, output_commit=None):
        """
        Inspects a job with a given ID. Returns a `JobInfo`.

        Params:
        * job_id: The ID of the job to inspect.
        * block_state: If true, block until the job completes.
        * output_commit: An optional tuple, string, or `Commit` object
        representing an output commit to filter on.
        """

        output_commit = commit_from(output_commit) if output_commit is not None else None
        req = proto.InspectJobRequest(job=proto.Job(id=job_id), block_state=block_state, output_commit=output_commit)
        return self.stub.InspectJob(req, metadata=self.metadata)

    def list_job(self, pipeline_name=None, input_commit=None, output_commit=None, history=None):
        """
        Lists jobs. Yields `JobInfo` objects.

        Params:
        * pipeline_name: An optional string representing a pipeline name to
        filter on.
        * input_commit: An optional list of tuples, strings, or `Commit`
        objects representing input commits to filter on.
        * output_commit: An optional tuple, string, or `Commit` object
        representing an output commit to filter on.
        * history: An optional int that indicates to return jobs from
        historical versions of pipelines. Semantics are:
         0: Return jobs from the current version of the pipeline or pipelines.
         1: Return the above and jobs from the next most recent version
         2: etc.
        -1: Return jobs from all historical versions.
        """

        pipeline = proto.Pipeline(name=pipeline_name) if pipeline_name is not None else None

        if isinstance(input_commit, list):
            input_commit = [commit_from(ic) for ic in input_commit]
        elif input_commit is not None:
            input_commit = [commit_from(input_commit)]

        output_commit = commit_from(output_commit) if output_commit is not None else None

        req = proto.ListJobRequest(pipeline=pipeline, input_commit=input_commit,
                                   output_commit=output_commit, history=history)

        return self.stub.ListJobStream(req, metadata=self.metadata)

    def delete_job(self, job_id):
        """
        Deletes a job by its ID.

        Params:
        * job_id: The ID of the job to delete.
        """

        req = proto.DeleteJobRequest(job=proto.Job(id=job_id))
        self.stub.DeleteJob(req, metadata=self.metadata)

    def stop_job(self, job_id):
        """
        Stops a job by its ID.

        Params:
        * job_id: The ID of the job to stop.
        """

        req = proto.StopJobRequest(job=proto.Job(id=job_id))
        self.stub.StopJob(req, metadata=self.metadata)

    def inspect_datum(self, job_id, datum_id):
        """
        Inspects a datum. Returns a `DatumInfo` object.

        Params:
        * job_id: The ID of the job.
        * datum_id: The ID of the datum.
        """

        req = proto.InspectDatumRequest(datum=proto.Datum(id=datum_id, job=proto.Job(id=job_id)))
        return self.stub.InspectDatum(req, metadata=self.metadata)

    def list_datum(self, job_id, page_size=None, page=None):
        """
        Lists datums. Yields `ListDatumStreamResponse` objects.

        Params:
        * job_id: The ID of the job.
        * page_size: An optional int specifying the size of the page.
        * page: An optional int specifying the page number.
        """

        req = proto.ListDatumRequest(job=proto.Job(id=job_id), page_size=page_size, page=page)
        return self.stub.ListDatumStream(req, metadata=self.metadata)

    def restart_datum(self, job_id, data_filters=None):
        """
        Restarts a datum.

        Params:
        * job_id: The ID of the job.
        * data_filters: An optional iterable of strings.
        """

        req = proto.RestartDatumRequest(job=proto.Job(id=job_id), data_filters=data_filters)
        self.stub.RestartDatum(req, metadata=self.metadata)

    def create_pipeline(self, pipeline_name, transform=None, parallelism_spec=None,
                        hashtree_spec=None, egress=None, update=None, output_branch=None,
                        scale_down_threshold=None, resource_requests=None,
                        resource_limits=None, input=None, description=None, cache_size=None,
                        enable_stats=None, reprocess=None, batch=None, max_queue_size=None,
                        service=None, chunk_spec=None, datum_timeout=None,
                        job_timeout=None, salt=None, standby=None, datum_tries=None,
                        scheduling_spec=None, pod_patch=None):
        """
        Creates a pipeline. For more info, please refer to the pipeline spec
        document:
        http://docs.pachyderm.io/en/latest/reference/pipeline_spec.html

        Params:
        * pipeline_name: A string representing the pipeline name.
        * transform: An optional `Transform` object.
        * parallelism_spec: An optional `ParallelismSpec` object.
        * hashtree_spec: An optional `HashtreeSpec` object.
        * egress: An optional `Egress` object.
        * update: An optional bool specifying whether this should behave as an
        upsert.
        * output_branch: An optional string representing the branch to output
        results on.
        * scale_down_threshold: An optional protobuf `Duration` object.
        * resource_requests: An optional `ResourceSpec` object.
        * resource_limits: An optional `ResourceSpec` object.
        * input: An optional `Input` object.
        * description: An optional string describing the pipeline.
        * cache_size: An optional string.
        * enable_stats: An optional bool.
        * reprocess: An optional bool. If true, pachyderm forces the pipeline
        to reprocess all datums. It only has meaning if `update` is `True`.
        * batch: An optional bool.
        * max_queue_size: An optional int.
        * service: An optional `Service` object.
        * chunk_spec: An optional `ChunkSpec` object.
        * datum_timeout: An optional protobuf `Duration` object.
        * job_timeout: An optional protobuf `Duration` object.
        * salt: An optional stirng.
        * standby: An optional bool.
        * datum_tries: An optional int.
        * scheduling_spec: An optional `SchedulingSpec` object.
        * pod_patch: An optional string.
        """

        req = proto.CreatePipelineRequest(
            pipeline=proto.Pipeline(name=pipeline_name),
            transform=transform, parallelism_spec=parallelism_spec,
            hashtree_spec=hashtree_spec, egress=egress, update=update,
            output_branch=output_branch, scale_down_threshold=scale_down_threshold,
            resource_requests=resource_requests, resource_limits=resource_limits,
            input=input, description=description, cache_size=cache_size,
            enable_stats=enable_stats, reprocess=reprocess, batch=batch,
            max_queue_size=max_queue_size, service=service,
            chunk_spec=chunk_spec, datum_timeout=datum_timeout,
            job_timeout=job_timeout, salt=salt, standby=standby,
            datum_tries=datum_tries, scheduling_spec=scheduling_spec,
            pod_patch=pod_patch
        )
        self.stub.CreatePipeline(req, metadata=self.metadata)

    def inspect_pipeline(self, pipeline_name):
        """
        Inspects a pipeline. Returns a `PipelineInfo` object.

        Params:
        * pipeline_name: A string representing the pipeline name.
        """
        req = proto.InspectPipelineRequest(pipeline=proto.Pipeline(name=pipeline_name))
        return self.stub.InspectPipeline(req, metadata=self.metadata)

    def list_pipeline(self):
        """
        Lists pipelines. Returns a `PipelineInfos` object.

        Params:
        * pipeline_name: A string representing the pipeline name.
        """
        req = proto.ListPipelineRequest()
        return self.stub.ListPipeline(req, metadata=self.metadata)

    def delete_pipeline(self, pipeline_name, force=None):
        """
        Deletes a pipeline.

        Params:
        * pipeline_name: A string representing the pipeline name.
        * force: Whether to force delete.
        """

        req = proto.DeletePipelineRequest(pipeline=proto.Pipeline(name=pipeline_name), force=force)
        self.stub.DeletePipeline(req, metadata=self.metadata)

    def delete_all_pipelines(self, force=None):
        """
        Deletes all pipelines.

        Params:
        * force: Whether to force delete.
        """

        req = proto.DeletePipelineRequest(all=True, force=force)
        self.stub.DeletePipeline(req, metadata=self.metadata)

    def start_pipeline(self, pipeline_name):
        """
        Starts a pipeline.

        Params:
        * pipeline_name: A string representing the pipeline name.
        """

        req = proto.StartPipelineRequest(pipeline=proto.Pipeline(name=pipeline_name))
        self.stub.StartPipeline(req, metadata=self.metadata)

    def stop_pipeline(self, pipeline_name):
        """
        Stops a pipeline.

        Params:
        * pipeline_name: A string representing the pipeline name.
        """
        req = proto.StopPipelineRequest(pipeline=proto.Pipeline(name=pipeline_name))
        self.stub.StopPipeline(req, metadata=self.metadata)

    def delete_all(self):
        """
        Deletes everything in pachyderm.
        """
        req = proto.google_dot_protobuf_dot_empty__pb2.Empty()
        self.stub.DeleteAll(req, metadata=self.metadata)

    def get_logs(self, pipeline_name=None, job_id=None, data_filters=None, master=None, datum=None, follow=None, tail=None):
        """
        Gets logs. Yields `LogMessage` objects.

        Params:
        * pipeline_name: An optional string representing a pipeline to get
        logs of.
        * job_id: An optional string representing a job to get logs of.
        * data_filters: An optional iterable of strings specifying the names
        of input files from which we want processing logs. This may contain
        multiple files, to query pipelines that contain multiple inputs. Each
        filter may be an absolute path of a file within a pps repo, or it may
        be a hash for that file (to search for files at specific versions.)
        * master: An optional bool.
        * datum: An optional `Datum` object.
        * follow: An optional bool specifying whether logs should continue to
        stream forever.
        * tail: An optional int. If nonzero, the number of lines from the end
        of the logs to return.  Note: tail applies per container, so you will
        get tail * <number of pods> total lines back.
        """

        pipeline = proto.Pipeline(name=pipeline_name) if pipeline_name else None
        job = proto.Job(id=job_id) if job_id else None

        if pipeline is None and job is None:
            raise ValueError("One of 'pipeline_name' or 'job_id' must be specified")

        req = proto.GetLogsRequest(
            pipeline=pipeline, job=job, data_filters=data_filters,
            master=master, datum=datum, follow=follow, tail=tail,
        )
        return self.stub.GetLogs(req, metadata=self.metadata)

    def garbage_collect(self):
        """
        Runs garbage collection.
        """
        return self.stub.GarbageCollect(proto.GarbageCollectRequest(), metadata=self.metadata)
