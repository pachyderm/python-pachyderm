# -*- coding: utf-8 -*-

import os
from builtins import object

from .pps_pb2 import *
from .pps_pb2_grpc import *


class PpsClient(object):
    def __init__(self,
                 host=os.environ.get('PACHD_SERVICE_HOST', 'localhost'),
                 port=os.environ.get('PACHD_SERVICE_PORT_API_GRPC_PORT', 30650)):
        """
        Creates a client to connect to Pfs
        :param host: The pachd host. Default is 'localhost', which is used with `pachctl port-forward`
        :param port: The port to connect to. Default is 30650
        """
        self.channel = grpc.insecure_channel('{}:{}'.format(host, port))
        self.stub = APIStub(self.channel)

    def create_job(self, transform, pipeline, pipeline_version, parallelism_spec, inputs, egress, service, output_repo,
                   output_branch, parent_job, resource_spec, input, new_branch, incremental, enable_stats, salt, batch):
        return self.stub.CreateJob(
            CreateJobRequest(transform=transform, pipeline=pipeline, pipeline_version=pipeline_version,
                             parallelism_spec=parallelism_spec, inputs=inputs, egress=egress, service=service,
                             output_repo=output_repo, output_branch=output_branch, parent_job=parent_job,
                             resource_spec=resource_spec, input=input, new_branch=new_branch, incremental=incremental,
                             enable_stats=enable_stats, salt=salt, batch=batch))

    def inspect_job(self, job_id, block_state=False):
        return self.stub.InspectJob(InspectJobRequest(job=Job(id=job_id), block_state=block_state))

    def list_job(self, pipeline=None, input_commit=None):
        return self.stub.ListJob(ListJobRequest(pipeline=pipeline, input_commit=input_commit))

    def delete_job(self, job_id):
        self.stub.DeleteJob(DeleteJobRequest(job=Job(id=job_id)))

    def stop_job(self, job_id):
        self.stub.StopJob(StopJobRequest(job=Job(id=job_id)))

    def inspect_datum(self, datum):
        return self.stub.InspectDatum(InspectDatumRequest(datum=datum))

    def list_datum(self, job_id):
        return self.stub.ListDatum(ListDatumRequest(job=Job(id=job_id)))

    def restart_datum(self, job_id, data_filters=tuple()):
        self.stub.RestartDatum(RestartDatumRequest(job=Job(id=job_id, data_filters=data_filters)))

    def create_pipeline(self, pipeline, transform, parallelism_spec, inputs, egress, update, output_branch,
                        scale_down_threshold, resource_spec, input, description, incremental, cache_size, enable_stats,
                        reprocess, batch):
        self.stub.CreatePipeline(
            CreatePipelineRequest(pipeline=pipeline, transform=transform, parallelism_spec=parallelism_spec,
                                  inputs=inputs, egress=egress, update=update, output_branch=output_branch,
                                  scale_down_threshold=scale_down_threshold, resource_spec=resource_spec, input=input,
                                  description=description, incremental=incremental, cache_size=cache_size,
                                  enable_stats=enable_stats, reprocess=reprocess, batch=batch))

    def inspect_pipeline(self, pipeline_name):
        return self.stub.InspectPipeline(InspectPipelineRequest(pipeline=Pipeline(name=pipeline_name)))

    def list_pipeline(self):
        return self.stub.ListPipeline(ListPipelineRequest())

    def delete_pipeline(self, pipeline_name, delete_jobs=False, delete_repo=False, all=False):
        self.stub.DeletePipeline(
            DeletePipelineRequest(pipeline=Pipeline(name=pipeline_name), delete_jobs=delete_jobs,
                                  delete_repo=delete_repo, all=all))

    def start_pipeline(self, pipeline_name):
        self.stub.StartPipeline(StartPipelineRequest(pipeline=Pipeline(name=pipeline_name)))

    def stop_pipeline(self, pipeline_name):
        self.stub.StopPipeline(StopPipelineRequest(pipeline=Pipeline(pipeline_name)))

    def rerun_pipeline(self, pipeline_name, exclude=tuple(), include=tuple()):
        self.stub.RerunPipeline(
            RerunPipelineRequest(pipeline=Pipeline(name=pipeline_name), exclude=exclude, include=include))

    def delete_all(self):
        self.stub.DeleteAll(google_dot_protobuf_dot_empty__pb2.Empty())

    def get_logs(self, pipeline_name=None, job_id=None, data_filters=tuple(), input_file_id='', master=False):
        pipeline = Pipeline(name=pipeline_name) if pipeline_name else None
        job = Job(id=job_id) if job_id else None
        if pipeline is None and job is None:
            raise ValueError("One of 'pipeline_name' or 'job_id' must be specified")
        return list(self.stub.GetLogs(
            GetLogsRequest(pipeline=pipeline, job=job, data_filters=data_filters,
                           input_file_id=input_file_id, master=master)))

    def garbage_collect(self):
        return GarbageCollectResponse(self.stub.GarbageCollect(GarbageCollectRequest()))
