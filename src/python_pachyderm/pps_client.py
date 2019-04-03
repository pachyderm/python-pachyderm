# -*- coding: utf-8 -*-

from __future__ import absolute_import

import os

from python_pachyderm.client.pps import pps_pb2 as proto
from python_pachyderm.client.pps import pps_pb2_grpc as grpc
from python_pachyderm.util import commit_from, get_address


class PpsClient(object):
    def __init__(self, host=None, port=None):
        """
        Creates a client to connect to Pfs
        :param host: The pachd host. Default is 'localhost', which is used with `pachctl port-forward`
        :param port: The port to connect to. Default is 30650
        """

        address = get_address(host, port)
        self.channel = grpc.grpc.insecure_channel(address)
        self.stub = grpc.APIStub(self.channel)

    def create_job(self, transform, pipeline, pipeline_version, parallelism_spec, inputs, egress, service, output_repo,
                   output_branch, parent_job, resource_spec, input, new_branch, incremental, enable_stats, salt, batch):
        return self.stub.CreateJob(proto.CreateJobRequest(
            transform=transform, pipeline=pipeline,
            pipeline_version=pipeline_version,
            parallelism_spec=parallelism_spec, inputs=inputs,
            egress=egress, service=service, output_repo=output_repo,
            output_branch=output_branch, parent_job=parent_job,
            resource_spec=resource_spec, input=input, new_branch=new_branch,
            incremental=incremental, enable_stats=enable_stats, salt=salt,
            batch=batch
        ))

    def inspect_job(self, job_id, block_state=False):
        return self.stub.InspectJob(proto.InspectJobRequest(job=proto.Job(id=job_id), block_state=block_state))

    def list_job(self, pipeline=None, input_commit=None):
        return self.stub.ListJob(proto.ListJobRequest(pipeline=pipeline, input_commit=commit_from(input_commit)))

    def delete_job(self, job_id):
        self.stub.DeleteJob(proto.DeleteJobRequest(job=proto.Job(id=job_id)))

    def stop_job(self, job_id):
        self.stub.StopJob(proto.StopJobRequest(job=proto.Job(id=job_id)))

    def inspect_datum(self, datum):
        return self.stub.InspectDatum(proto.InspectDatumRequest(datum=datum))

    def list_datum(self, job_id):
        return self.stub.ListDatum(proto.ListDatumRequest(job=proto.Job(id=job_id)))

    def restart_datum(self, job_id, data_filters=tuple()):
        self.stub.RestartDatum(proto.RestartDatumRequest(job=proto.Job(id=job_id, data_filters=data_filters)))

    def create_pipeline(self, pipeline_name, transform=None, parallelism_spec=None,
                        hashtree_spec=None, egress=None, update=None, output_branch=None,
                        scale_down_threshold=None, resource_requests=None,
                        resource_limits=None, input=None, description=None, cache_size=None,
                        enable_stats=None, reprocess=None, batch=None, max_queue_size=None,
                        service=None, chunk_spec=None, datum_timeout=None,
                        job_timeout=None, salt=None, standby=None, datum_tries=None,
                        scheduling_spec=None, pod_spec=None, pod_patch=None):
        self.stub.CreatePipeline(proto.CreatePipelineRequest(
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
        ))

    def inspect_pipeline(self, pipeline_name):
        return self.stub.InspectPipeline(proto.InspectPipelineRequest(pipeline=proto.Pipeline(name=pipeline_name)))

    def list_pipeline(self):
        return self.stub.ListPipeline(proto.ListPipelineRequest())

    def delete_pipeline(self, pipeline_name, delete_jobs=False, delete_repo=False, all=False):
        self.stub.DeletePipeline(proto.DeletePipelineRequest(
            pipeline=proto.Pipeline(name=pipeline_name),
            delete_jobs=delete_jobs,
            delete_repo=delete_repo,
            all=all
        ))

    def start_pipeline(self, pipeline_name):
        self.stub.StartPipeline(proto.StartPipelineRequest(pipeline=proto.Pipeline(name=pipeline_name)))

    def stop_pipeline(self, pipeline_name):
        self.stub.StopPipeline(proto.StopPipelineRequest(pipeline=proto.Pipeline(pipeline_name)))

    def rerun_pipeline(self, pipeline_name, exclude=tuple(), include=tuple()):
        self.stub.RerunPipeline(proto.RerunPipelineRequest(
            pipeline=proto.Pipeline(name=pipeline_name),
            exclude=exclude,
            include=include
        ))

    def delete_all(self):
        self.stub.DeleteAll(proto.google_dot_protobuf_dot_empty__pb2.Empty())

    def get_logs(self, pipeline_name=None, job_id=None, data_filters=tuple(), master=False):
        pipeline = proto.Pipeline(name=pipeline_name) if pipeline_name else None
        job = proto.Job(id=job_id) if job_id else None
        
        if pipeline is None and job is None:
            raise ValueError("One of 'pipeline_name' or 'job_id' must be specified")
        
        return list(self.stub.GetLogs(
            proto.GetLogsRequest(
                pipeline=pipeline, job=job, data_filters=data_filters,
                master=master
            )
        ))

    def garbage_collect(self):
        return proto.GarbageCollectResponse(self.stub.GarbageCollect(proto.GarbageCollectRequest()))
