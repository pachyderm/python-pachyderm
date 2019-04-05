# -*- coding: utf-8 -*-

from __future__ import absolute_import

import os

from python_pachyderm.client.pps import pps_pb2 as proto
from python_pachyderm.client.pps import pps_pb2_grpc as grpc
from python_pachyderm.util import commit_from, get_address, get_metadata


class PpsClient(object):
    def __init__(self, host=None, port=None, auth_token=None):
        """
        Creates a client to connect to Pfs
        :param host: The pachd host. Default is 'localhost', which is used with `pachctl port-forward`
        :param port: The port to connect to. Default is 30650
        :param auth_token: The authentication token; used if authentication is enabled on the cluster. Default to `None`.
        """

        address = get_address(host, port)
        self.metadata = get_metadata(auth_token)
        self.channel = grpc.grpc.insecure_channel(address)
        self.stub = grpc.APIStub(self.channel)

    def create_job(self, transform, pipeline, pipeline_version, parallelism_spec, inputs, egress, service, output_repo,
                   output_branch, parent_job, resource_spec, input, new_branch, incremental, enable_stats, salt, batch):
        req = proto.CreateJobRequest(
            transform=transform, pipeline=pipeline,
            pipeline_version=pipeline_version,
            parallelism_spec=parallelism_spec, inputs=inputs,
            egress=egress, service=service, output_repo=output_repo,
            output_branch=output_branch, parent_job=parent_job,
            resource_spec=resource_spec, input=input, new_branch=new_branch,
            incremental=incremental, enable_stats=enable_stats, salt=salt,
            batch=batch
        )
        return self.stub.CreateJob(req, metadata=self.metadata)

    def inspect_job(self, job_id, block_state=False):
        req = proto.InspectJobRequest(job=proto.Job(id=job_id), block_state=block_state)
        return self.stub.InspectJob(req, metadata=self.metadata)

    def list_job(self, pipeline=None, input_commit=None):
        req = proto.ListJobRequest(pipeline=pipeline, input_commit=commit_from(input_commit))
        return self.stub.ListJob(req, metadata=self.metadata)

    def delete_job(self, job_id):
        req = proto.DeleteJobRequest(job=proto.Job(id=job_id))
        self.stub.DeleteJob(req, metadata=self.metadata)

    def stop_job(self, job_id):
        req = proto.StopJobRequest(job=proto.Job(id=job_id))
        self.stub.StopJob(req, metadata=self.metadata)

    def inspect_datum(self, datum):
        req = proto.InspectDatumRequest(datum=datum)
        return self.stub.InspectDatum(req, metadata=self.metadata)

    def list_datum(self, job_id):
        req = proto.ListDatumRequest(job=proto.Job(id=job_id))
        return self.stub.ListDatum(req, metadata=self.metadata)

    def restart_datum(self, job_id, data_filters=tuple()):
        req = proto.RestartDatumRequest(job=proto.Job(id=job_id, data_filters=data_filters))
        self.stub.RestartDatum(req, metadata=self.metadata)

    def create_pipeline(self, pipeline_name, transform=None, parallelism_spec=None,
                        hashtree_spec=None, egress=None, update=None, output_branch=None,
                        scale_down_threshold=None, resource_requests=None,
                        resource_limits=None, input=None, description=None, cache_size=None,
                        enable_stats=None, reprocess=None, batch=None, max_queue_size=None,
                        service=None, chunk_spec=None, datum_timeout=None,
                        job_timeout=None, salt=None, standby=None, datum_tries=None,
                        scheduling_spec=None, pod_spec=None, pod_patch=None):
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
            pod_spec=pod_spec, pod_patch=pod_patch
        )
        self.stub.CreatePipeline(req, metadata=self.metadata)

    def inspect_pipeline(self, pipeline_name):
        req = proto.InspectPipelineRequest(pipeline=proto.Pipeline(name=pipeline_name))
        return self.stub.InspectPipeline(req, metadata=self.metadata)

    def list_pipeline(self):
        req = proto.ListPipelineRequest()
        return self.stub.ListPipeline(req, metadata=self.metadata)

    def delete_pipeline(self, pipeline_name, delete_jobs=False, delete_repo=False, all=False):
        req = proto.DeletePipelineRequest(
            pipeline=proto.Pipeline(name=pipeline_name),
            delete_jobs=delete_jobs,
            delete_repo=delete_repo,
            all=all
        )
        self.stub.DeletePipeline(req, metadata=self.metadata)

    def start_pipeline(self, pipeline_name):
        req = proto.StartPipelineRequest(pipeline=proto.Pipeline(name=pipeline_name))
        self.stub.StartPipeline(req, metadata=self.metadata)

    def stop_pipeline(self, pipeline_name):
        req = proto.StopPipelineRequest(pipeline=proto.Pipeline(pipeline_name))
        self.stub.StopPipeline(req, metadata=self.metadata)

    def rerun_pipeline(self, pipeline_name, exclude=tuple(), include=tuple()):
        req = proto.RerunPipelineRequest(
            pipeline=proto.Pipeline(name=pipeline_name),
            exclude=exclude,
            include=include
        )
        self.stub.RerunPipeline(req, metadata=self.metadata)

    def delete_all(self):
        req = proto.google_dot_protobuf_dot_empty__pb2.Empty()
        self.stub.DeleteAll(req, metadata=self.metadata)

    def get_logs(self, pipeline_name=None, job_id=None, data_filters=tuple(), master=False):
        pipeline = proto.Pipeline(name=pipeline_name) if pipeline_name else None
        job = proto.Job(id=job_id) if job_id else None
        
        if pipeline is None and job is None:
            raise ValueError("One of 'pipeline_name' or 'job_id' must be specified")
        
        req = proto.GetLogsRequest(
            pipeline=pipeline, job=job, data_filters=data_filters,
            master=master
        )
        return list(self.stub.GetLogs(req, metadata=self.metadata))

    def garbage_collect(self):
        req = self.stub.GarbageCollect(proto.GarbageCollectRequest())
        return proto.GarbageCollectResponse(req, metadata=self.metadata)
