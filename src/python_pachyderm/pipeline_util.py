import io
import os
import sys
import json
import uuid
import tarfile
import tempfile
import collections

from .client import Client
from .proto.pps.pps_pb2 import Transform

DOCKERFILE_TEMPLATE = """
FROM {base}
COPY . /app
WORKDIR /app
{install_deps_cmd}
"""

CreateImageStream = collections.namedtuple("CreateImageStream", ["json"])
CreateImageFinished = collections.namedtuple("CreateImageFinishedResult", ["repository", "tag"])

def build_pipeline(path, image_name, input, pipeline_name=None, docker_registry=None, docker_client=None, pachyderm_client=None, image_pull_secrets=None, debug=None, pipeline_kwargs=None):
    builder = PipelineBuilder(path, image_name, pipeline_name=pipeline_name)

    if docker_client is None:
        import docker
        docker_client = docker.APIClient()
    
    if pachyderm_client is None:
        if "PACHD_SERVICE_HOST" in os.environ and "PACHD_SERVICE_PORT" in os.environ:
            pachyderm_client = Client.new_in_cluster()
        else:
            pachyderm_client = Client()

    image = None
    for update in builder.create_image(docker_client, docker_registry=docker_registry):
        if isinstance(update, CreateImageStream):
            if "stream" in update.json:
                # stream will already contain newlines, so don't use print()
                sys.stdout.write(update.json["stream"])
            elif "status" in update.json:
                if "id" in update.json:
                    if "progress" in update.json:
                        print("{} {} {}".format(update.json["status"], update.json["id"], update.json["progress"]))
                    else:
                        print("{} {}".format(update.json["status"], update.json["id"]))
                else:
                    print(update.json["status"])
            else:
                if "aux" not in update.json:
                    print(update.json)
        elif isinstance(update, CreateImageFinished):
            image = "{}:{}".format(update.repository, update.tag)
    assert image is not None, "did not receive a CreateImageFinished object"

    return builder.create_pipeline(
        pachyderm_client, image, input,
        image_pull_secrets=image_pull_secrets,
        debug=debug,
        **(pipeline_kwargs or {})
    )

def _stream_docker_logs(source):
    for lines in source:
        for line in lines.decode("utf8").split("\n"):
            line = line.strip()
            if line:
                j = json.loads(line)
                if "error" in j:
                    raise Exception(j["error"])
                yield CreateImageStream(json=j)

class PipelineBuilder:
    def __init__(self, path, image_name, pipeline_name=None):
        self.path = path
        if not os.path.exists(path):
            raise Exception("path does not exist")

        self.image_name = image_name

        if pipeline_name is not None:
            self.pipeline_name = pipeline_name
        elif not path.endswith("/"):
            self.pipeline_name = os.path.basename(path)
        else:
            self.pipeline_name = os.path.basename(path[:-1])

        if not self.pipeline_name:
            raise Exception("could not derive pipeline name")

    def create_image(self, docker_client, docker_registry=None):
        docker_registry = docker_registry or "index.docker.io"
        exists = lambda f: os.path.exists(os.path.join(self.path, f))
        tag = uuid.uuid4().hex

        if not exists("main.py"):
            raise Exception("no main.py found")

        if exists("base.txt"):
            with open(os.path.join(self.path, "base.txt"), "r") as f:
                base = f.read().strip()
        else:
            base = "python:3.7"

        install_deps_cmd = "RUN pip3 install -r requirements.txt" if exists("requirements.txt") else ""

        dockerfile_contents = DOCKERFILE_TEMPLATE.format(
            base=base,
            install_deps_cmd=install_deps_cmd,
        ).encode("utf8")

        with tempfile.NamedTemporaryFile(prefix="python_pachyderm_pipeline_builder", suffix=".tgz") as context_file:
            with tarfile.open(context_file.name, "w:gz") as tar:
                tar.add(self.path, arcname=".")

                with io.BytesIO(dockerfile_contents) as dockerfile:
                    dockefile_info = tarfile.TarInfo(name="Dockerfile")
                    dockefile_info.size = len(dockerfile_contents)
                    tar.addfile(dockefile_info, fileobj=dockerfile)

            build_logs = docker_client.build(fileobj=context_file, custom_context=True, encoding="gzip", tag=self.image_name)
            yield from _stream_docker_logs(build_logs)

        repository = "{}/{}".format(docker_registry, self.image_name)
        
        if not docker_client.tag(self.image_name, repository, tag):
            raise Exception("tagging failed")

        push_logs = docker_client.push(repository, tag, stream=True)
        yield from _stream_docker_logs(push_logs)

        yield CreateImageFinished(repository=repository, tag=tag)

    def create_pipeline(self, pachyderm_client, image, input, image_pull_secrets=None, debug=None, **kwargs):
        transform = Transform(
            image=image,
            cmd=["python3", "main.py"],
            image_pull_secrets=image_pull_secrets,
            debug=debug,
        )
        return pachyderm_client.create_pipeline(self.pipeline_name, transform, input=input, **kwargs)
