import os
import collections
import itertools
from contextlib import contextmanager

from python_pachyderm.proto.pfs import pfs_pb2 as pfs_proto
from python_pachyderm.proto.pfs import pfs_pb2_grpc as pfs_grpc
from python_pachyderm.proto.pps import pps_pb2 as pps_proto
from python_pachyderm.proto.pps import pps_pb2_grpc as pps_grpc
from python_pachyderm.proto.version.versionpb import version_pb2_grpc as version_grpc


BUFFER_SIZE = 3 * 1024 * 1024  # 3MB TODO: Base this on some grpc value


def commit_from(src, allow_just_repo=False):
    if isinstance(src, pfs_proto.Commit):
        return src
    elif isinstance(src, (tuple, list)) and len(src) == 2:
        return pfs_proto.Commit(repo=pfs_proto.Repo(name=src[0]), id=src[1])
    elif isinstance(src, str):
        repo_name, commit_id = src.split('/', 1)
        return pfs_proto.Commit(repo=pfs_proto.Repo(name=repo_name), id=commit_id)

    if not allow_just_repo:
        raise ValueError("Invalid commit type")
    return pfs_proto.Commit(repo=pfs_proto.Repo(name=src))


class Client(object):
    def __init__(self, host=None, port=None, auth_token=None, root_certs=None):
        """
        Creates a client to connect to PFS.

        Params:

        * `host`: The pachd host. Default is 'localhost', which is used with
        `pachctl port-forward`.
        * `port`: The port to connect to. Default is 30650.
        * `auth_token`: The authentication token; used if authentication is
        enabled on the cluster. Default to `None`.
        * `root_certs`:  The PEM-encoded root certificates as byte string.
        """

        if host is not None and port is not None:
            self.address = "{}:{}".format(host, port)
        else:
            self.address = os.environ.get("PACHD_ADDRESS", "localhost:30650")

        if auth_token is None:
            auth_token = os.environ.get("PACH_PYTHON_AUTH_TOKEN")

        self.metadata = []
        if auth_token is not None:
            self.metadata.append(("authn-token", auth_token))

        self.root_certs = root_certs

    @property
    def _pfs_stub(self):
        if not hasattr(self, "__pfs_stub"):
            if self.root_certs:
                ssl_channel_credentials = pfs_grpc.grpc.ssl_channel_credentials
                ssl = ssl_channel_credentials(root_certificates=self.root_certs)
                channel = pfs_grpc.grpc.secure_channel(self.address, ssl)
            else:
                channel = pfs_grpc.grpc.insecure_channel(self.address)
            self.__pfs_stub = pfs_grpc.APIStub(channel)
        return self.__pfs_stub

    @property
    def _pps_stub(self):
        if not hasattr(self, "__pps_stub"):
            if self.root_certs:
                ssl_channel_credentials = pps_grpc.grpc.ssl_channel_credentials
                ssl = ssl_channel_credentials(root_certificates=self.root_certs)
                channel = pps_grpc.grpc.secure_channel(self.address, ssl)
            else:
                channel = pps_grpc.grpc.insecure_channel(self.address)
            self.__pps_stub = pps_grpc.APIStub(channel)
        return self.__pps_stub

    @property
    def _version_stub(self):
        if not hasattr(self, "__version_stub"):
            if self.root_certs:
                ssl_channel_credentials = version_grpc.grpc.ssl_channel_credentials
                ssl = ssl_channel_credentials(root_certificates=self.root_certs)
                channel = version_grpc.grpc.secure_channel(self.address, ssl)
            else:
                channel = version_grpc.grpc.insecure_channel(self.address)
            self.__version_stub = version_grpc.APIStub(channel)
        return self.__version_stub

    def get_remote_version(self):
        req = version_grpc.google_dot_protobuf_dot_empty__pb2.Empty()
        return self._version_stub.GetVersion(req, metadata=self.metadata)

    def create_repo(self, repo_name, description=None, update=None):
        """
        Creates a new `Repo` object in PFS with the given name. Repos are the
        top level data object in PFS and should be used to store data of a
        similar type. For example rather than having a single `Repo` for an
        entire project you might have separate `Repo`s for logs, metrics,
        database dumps etc.

        Params:

        * `repo_name`: Name of the repo.
        * `description`: An optional string describing the repo.
        * `update`: Whether to update if the repo already exists.
        """
        req = pfs_proto.CreateRepoRequest(
            repo=pfs_proto.Repo(name=repo_name),
            description=description,
            update=update
        )
        self._pfs_stub.CreateRepo(req, metadata=self.metadata)

    def inspect_repo(self, repo_name):
        """
        Returns info about a specific repo. Returns a `RepoInfo` object.

        Params:
        * `repo_name`: Name of the repo.
        """
        req = pfs_proto.InspectRepoRequest(repo=pfs_proto.Repo(name=repo_name))
        return self._pfs_stub.InspectRepo(req, metadata=self.metadata)

    def list_repo(self):
        """
        Returns info about all repos, as a list of `RepoInfo` objects.
        """
        req = pfs_proto.ListRepoRequest()
        res = self._pfs_stub.ListRepo(req, metadata=self.metadata)
        return res.repo_info

    def delete_repo(self, repo_name, force=None):
        """
        Deletes a repo and reclaims the storage space it was using.

        Params:

        * `repo_name`: The name of the repo.
        * `force`: If set to true, the repo will be removed regardless of
        errors. This argument should be used with care.
        """
        req = pfs_proto.DeleteRepoRequest(repo=pfs_proto.Repo(name=repo_name), force=force, all=False)
        self._pfs_stub.DeleteRepo(req, metadata=self.metadata)

    def delete_all_repos(self, force=None):
        """
        Deletes all repos.

        Params:

        * `force`: If set to true, the repo will be removed regardless of
        errors. This argument should be used with care.
        """

        req = pfs_proto.DeleteRepoRequest(force=force, all=True)
        self._pfs_stub.DeleteRepo(req, metadata=self.metadata)

    def start_commit(self, repo_name, branch=None, parent=None, description=None, provenance=None):
        """
        Begins the process of committing data to a Repo. Once started you can
        write to the Commit with PutFile and when all the data has been
        written you must finish the Commit with FinishCommit. NOTE, data is
        not persisted until FinishCommit is called. A Commit object is
        returned.

        Params:

        * `repo_name`: A string specifying the name of the repo.
        * `branch`: A string specifying the branch name. This is a more
        convenient way to build linear chains of commits. When a commit is
        started with a non-empty branch the value of branch becomes an alias
        for the created Commit. This enables a more intuitive access pattern.
        When the commit is started on a branch the previous head of the branch
        is used as the parent of the commit.
        * `parent`: An optional `Commit` object specifying the parent commit.
        Upon creation the new commit will appear identical to the parent
        commit, data can safely be added to the new commit without affecting
        the contents of the parent commit.
        * `description`: An optional string describing the commit.
        * `provenance`: An optional iterable of `CommitProvenance` objects
        specifying the commit provenance.
        """
        req = pfs_proto.StartCommitRequest(
            parent=pfs_proto.Commit(repo=pfs_proto.Repo(name=repo_name), id=parent),
            branch=branch,
            description=description,
            provenance=provenance,
        )
        return self._pfs_stub.StartCommit(req, metadata=self.metadata)

    def finish_commit(self, commit, description=None,
                      tree_object_hashes=None, datum_object_hash=None,
                      size_bytes=None, empty=None):
        """
        Ends the process of committing data to a Repo and persists the
        Commit. Once a Commit is finished the data becomes immutable and
        future attempts to write to it with PutFile will error.

        Params:

        * `commit`: A tuple, string, or `Commit` object representing the
        commit.
        * `description`: An optional string describing this commit.
        * `tree_object_hashes`: A list of zero or more strings specifying
        object hashes.
        * `datum_object_hash`: An optional string specifying an object hash.
        * `size_bytes`: An optional int.
        * `empty`: An optional bool. If set, the commit will be closed (its
        `finished` field will be set to the current time) but its `tree` will
        be left nil.
        """
        req = pfs_proto.FinishCommitRequest(
            commit=commit_from(commit),
            description=description,
            trees=[pfs_proto.Object(hash=h) for h in tree_object_hashes] if tree_object_hashes is not None else None,
            datums=pfs_proto.Object(hash=datum_object_hash) if datum_object_hash is not None else None,
            size_bytes=size_bytes,
            empty=empty,
        )
        return self._pfs_stub.FinishCommit(req, metadata=self.metadata)

    @contextmanager
    def commit(self, repo_name, branch=None, parent=None, description=None):
        """
        A context manager for running operations within a commit.

        Params:

        * `repo_name`: A string specifying the name of the repo.
        * `branch`: A string specifying the branch name. This is a more
        convenient way to build linear chains of commits. When a commit is
        started with a non-empty branch the value of branch becomes an alias
        for the created Commit. This enables a more intuitive access pattern.
        When the commit is started on a branch the previous head of the branch
        is used as the parent of the commit.
        * `parent`: An optional `Commit` object specifying the parent commit.
        Upon creation the new commit will appear identical to the parent
        commit, data can safely be added to the new commit without affecting
        the contents of the parent commit.
        * `description`: An optional string describing the commit.
        """
        commit = self.start_commit(repo_name, branch, parent, description)
        try:
            yield commit
        finally:
            self.finish_commit(commit)

    def inspect_commit(self, commit, block_state=None):
        """
        Inspects a commit. Returns a `CommitInfo` object.

        Params:

        * `commit`: A tuple, string, or `Commit` object representing the
        commit.
        * `block_state`: Causes inspect commit to block until the commit is in
        the desired commit state.
        """
        req = pfs_proto.InspectCommitRequest(commit=commit_from(commit), block_state=block_state)
        return self._pfs_stub.InspectCommit(req, metadata=self.metadata)

    def list_commit(self, repo_name, to_commit=None, from_commit=None, number=None):
        """
        Lists commits. Yields `CommitInfo` objects.

        Params:

        * `repo_name`: If only `repo_name` is given, all commits in the repo
        are returned.
        * `to_commit`: Optional. Only the ancestors of `to`, including `to`
        itself, are considered.
        * `from_commit`: Optional. Only the descendants of `from`, including
        `from` itself, are considered.
        * `number`: Optional. Determines how many commits are returned.  If
        `number` is 0, all commits that match the aforementioned criteria are
        returned.
        """
        req = pfs_proto.ListCommitRequest(repo=pfs_proto.Repo(name=repo_name), number=number)
        if to_commit is not None:
            req.to.CopyFrom(commit_from(to_commit))
        if from_commit is not None:
            getattr(req, 'from').CopyFrom(commit_from(from_commit))
        return self._pfs_stub.ListCommitStream(req, metadata=self.metadata)

    def delete_commit(self, commit):
        """
        Deletes a commit.

        Params:

        * `commit`: A tuple, string, or `Commit` object representing the
        commit.
        """
        req = pfs_proto.DeleteCommitRequest(commit=commit_from(commit))
        self._pfs_stub.DeleteCommit(req, metadata=self.metadata)

    def flush_commit(self, commits, repos=None):
        """
        Blocks until all of the commits which have a set of commits as
        provenance have finished. For commits to be considered they must have
        all of the specified commits as provenance. This in effect waits for
        all of the jobs that are triggered by a set of commits to complete.
        It returns an error if any of the commits it's waiting on are
        cancelled due to one of the jobs encountering an error during runtime.
        Note that it's never necessary to call FlushCommit to run jobs,
        they'll run no matter what, FlushCommit just allows you to wait for
        them to complete and see their output once they do. This returns an
        iterator of CommitInfo objects.

        Yields `CommitInfo` objects.

        Params:

        * `commits`: A list of tuples, strings, or `Commit` objects
        representing the commits to flush.
        * `repos`: An optional list of strings specifying repo names. If
        specified, only commits within these repos will be flushed.
        """
        to_repos = [pfs_proto.Repo(name=r) for r in repos] if repos is not None else None
        req = pfs_proto.FlushCommitRequest(commits=[commit_from(c) for c in commits],
                                           to_repos=to_repos)
        return self._pfs_stub.FlushCommit(req, metadata=self.metadata)

    def subscribe_commit(self, repo_name, branch, from_commit_id=None, state=None):
        """
        Yields `CommitInfo` objects as commits occur.

        Params:

        * `repo_name`: A string specifying the name of the repo.
        * `branch`: A string specifying branch to subscribe to.
        * `from_commit_id`: An optional string specifying the commit ID. Only
        commits created since this commit are returned.
        * `state`: The commit state to filter on.
        """
        repo = pfs_proto.Repo(name=repo_name)
        req = pfs_proto.SubscribeCommitRequest(repo=repo, branch=branch, state=state)
        if from_commit_id is not None:
            getattr(req, 'from').CopyFrom(pfs_proto.Commit(repo=repo, id=from_commit_id))
        return self._pfs_stub.SubscribeCommit(req, metadata=self.metadata)

    def create_branch(self, repo_name, branch_name, commit=None, provenance=None):
        """
        Creates a new branch.

        Params:
        * `repo_name`: A string specifying the name of the repo.
        * `branch_name`: A string specifying the new branch name.
        * `commit`: An optional tuple, string, or `Commit` object representing
        the head commit of the branch.
        * `provenance`: An optional iterable of `Branch` objects representing
        the branch provenance.
        """
        req = pfs_proto.CreateBranchRequest(
            branch=pfs_proto.Branch(repo=pfs_proto.Repo(name=repo_name), name=branch_name),
            head=commit_from(commit) if commit is not None else None,
            provenance=provenance,
        )
        self._pfs_stub.CreateBranch(req, metadata=self.metadata)

    def inspect_branch(self, repo_name, branch_name):
        """
        Inspects a branch. Returns a `BranchInfo` object.
        """
        branch = pfs_proto.Branch(repo=pfs_proto.Repo(name=repo_name), name=branch_name)
        req = pfs_proto.InspectBranchRequest(branch=branch)
        return self._pfs_stub.InspectBranch(req, metadata=self.metadata)

    def list_branch(self, repo_name):
        """
        Lists the active branch objects on a repo. Returns a list of
        `BranchInfo` objects.

        Params:

        * `repo_name`: A string specifying the repo name.
        """
        req = pfs_proto.ListBranchRequest(repo=pfs_proto.Repo(name=repo_name))
        res = self._pfs_stub.ListBranch(req, metadata=self.metadata)
        return res.branch_info

    def delete_branch(self, repo_name, branch_name, force=None):
        """
        Deletes a branch, but leaves the commits themselves intact. In other
        words, those commits can still be accessed via commit IDs and other
        branches they happen to be on.

        Params:

        * `repo_name`: A string specifying the repo name.
        * `branch_name`: A string specifying the name of the branch to delete.
        * `force`: A bool specifying whether to force the branch deletion.
        """
        branch = pfs_proto.Branch(repo=pfs_proto.Repo(name=repo_name), name=branch_name)
        req = pfs_proto.DeleteBranchRequest(branch=branch, force=force)
        self._pfs_stub.DeleteBranch(req, metadata=self.metadata)

    def put_file_bytes(self, commit, path, value, delimiter=None,
                       target_file_datums=None, target_file_bytes=None, overwrite_index=None):
        """
        Uploads a binary bytes array as file(s) in a certain path.

        Params:

        * `commit`: A tuple, string, or `Commit` object representing the
        commit.
        * `path`: A string specifying the path in the repo the file(s) will be
        written to.
        * `value`: The file contents as bytes, represented as a file-like
        object, bytestring, or iterator of bytestrings.
        * `delimiter`: Optional. causes data to be broken up into separate
        files with `path` as a prefix.
        * `target_file_datums`: An optional int. Specifies the target number of
        datums in each written file. It may be lower if data does not split
        evenly, but will never be higher, unless the value is 0.
        * `target_file_bytes`: An optional int. Specifies the target number of
        bytes in each written file, files may have more or fewer bytes than
        the target.
        * `overwrite_index`: An optional `OverwriteIndex` object. This is the
        object index where the write starts from.  All existing objects
        starting from the index are deleted.
        """

        overwrite_index = pfs_proto.OverwriteIndex(index=overwrite_index) if overwrite_index is not None else None

        if hasattr(value, "read"):
            def wrap(value):
                for i in itertools.count():
                    chunk = value.read(BUFFER_SIZE)

                    if len(chunk) == 0:
                        return

                    if i == 0:
                        yield pfs_proto.PutFileRequest(
                            file=pfs_proto.File(commit=commit_from(commit), path=path),
                            value=chunk,
                            delimiter=delimiter,
                            target_file_datums=target_file_datums,
                            target_file_bytes=target_file_bytes,
                            overwrite_index=overwrite_index
                        )
                    else:
                        yield pfs_proto.PutFileRequest(value=chunk)
        elif isinstance(value, collections.Iterable) and not isinstance(value, (str, bytes)):
            def wrap(value):
                for i, chunk in enumerate(value):
                    if i == 0:
                        yield pfs_proto.PutFileRequest(
                            file=pfs_proto.File(commit=commit_from(commit), path=path),
                            value=chunk,
                            delimiter=delimiter,
                            target_file_datums=target_file_datums,
                            target_file_bytes=target_file_bytes,
                            overwrite_index=overwrite_index
                        )
                    else:
                        yield pfs_proto.PutFileRequest(value=chunk)
        else:
            def wrap(value):
                yield pfs_proto.PutFileRequest(
                    file=pfs_proto.File(commit=commit_from(commit), path=path),
                    value=value[:BUFFER_SIZE],
                    delimiter=delimiter,
                    target_file_datums=target_file_datums,
                    target_file_bytes=target_file_bytes,
                    overwrite_index=overwrite_index
                )

                for i in range(BUFFER_SIZE, len(value), BUFFER_SIZE):
                    yield pfs_proto.PutFileRequest(
                        value=value[i:i + BUFFER_SIZE],
                        overwrite_index=overwrite_index
                    )

        self._pfs_stub.PutFile(wrap(value), metadata=self.metadata)

    def put_file_url(self, commit, path, url, recursive=None, overwrite_index=None):
        """
        Puts a file using the content found at a URL. The URL is sent to the
        server which performs the request. Note that this is not a standard
        PFS function.

        Params:

        * `commit`: A tuple, string, or `Commit` object representing the
        commit.
        * `path`: A string specifying the path to the file.
        * `url`: A string specifying the url of the file to put.
        * `recursive`: allow for recursive scraping of some types URLs, for
        example on s3:// URLs.
        * `overwrite_index`: An optional `OverwriteIndex` object. This is the
        object index where the write starts from.  All existing objects
        starting from the index are deleted.
        """

        overwrite_index = pfs_proto.OverwriteIndex(index=overwrite_index) if overwrite_index is not None else None

        req = iter([
            pfs_proto.PutFileRequest(
                file=pfs_proto.File(commit=commit_from(commit), path=path),
                url=url,
                recursive=recursive,
                overwrite_index=overwrite_index
            )
        ])
        self._pfs_stub.PutFile(req, metadata=self.metadata)

    def copy_file(self, source_commit, source_path, dest_commit, dest_path, overwrite=None):
        """
        Efficiently copies files already in PFS. Note that the destination
        repo cannot be an output repo, or the copy operation will (as of
        1.9.0) silently fail.

        Params:

        * `source_commit`: A tuple, string, or `Commit` object representing the
        commit for the source file.
        * `source_path`: A string specifying the path of the source file.
        * `dest_commit`: A tuple, string, or `Commit` object representing the
        commit for the destination file.
        * `dest_path`: A string specifying the path of the destination file.
        * `overwrite`: Am optional bool specifying whether to overwrite the
        destination file if it already exists.
        """
        req = pfs_proto.CopyFileRequest(
            src=pfs_proto.File(commit=commit_from(source_commit), path=source_path),
            dst=pfs_proto.File(commit=commit_from(dest_commit), path=dest_path),
            overwrite=overwrite,
        )
        self._pfs_stub.CopyFile(req, metadata=self.metadata)

    def get_file(self, commit, path, offset_bytes=None, size_bytes=None):
        """
        Returns an iterator of the contents of a file at a specific commit.

        Params:

        * `commit`: A tuple, string, or `Commit` object representing the
        commit.
        * `path`: A string specifying the path of the file.
        * `offset_bytes`: An optional int. Specifies a number of bytes that
        should be skipped in the beginning of the file.
        * `size_bytes`: An optional int. limits the total amount of data
        returned, note you will get fewer bytes than size if you pass a value
        larger than the size of the file. If size is set to 0 then all of the
        data will be returned.
        """
        req = pfs_proto.GetFileRequest(
            file=pfs_proto.File(commit=commit_from(commit), path=path),
            offset_bytes=offset_bytes,
            size_bytes=size_bytes
        )
        res = self._pfs_stub.GetFile(req, metadata=self.metadata)
        for item in res:
            yield item.value

    def inspect_file(self, commit, path):
        """
        Inspects a file. Returns a `FileInfo` object.

        Params:

        * `commit`: A tuple, string, or `Commit` object representing the
        commit.
        * `path`: A string specifying the path to the file.
        """
        req = pfs_proto.InspectFileRequest(file=pfs_proto.File(commit=commit_from(commit), path=path))
        return self._pfs_stub.InspectFile(req, metadata=self.metadata)

    def list_file(self, commit, path, history=None, include_contents=None):
        """
        Lists the files in a directory.

        Params:

        * `commit`: A tuple, string, or `Commit` object representing the
        commit.
        * `path`: The path to the directory.
        * `history`: An optional int that indicates to return jobs from
        historical versions of pipelines. Semantics are:
         0: Return jobs from the current version of the pipeline or pipelines.
         1: Return the above and jobs from the next most recent version
         2: etc.
        -1: Return jobs from all historical versions.
        * `include_contents`: An optional bool. If `True`, file contents are
        included.
        """

        req = pfs_proto.ListFileRequest(
            file=pfs_proto.File(commit=commit_from(commit), path=path),
            history=history,
            full=include_contents,
        )

        return self._pfs_stub.ListFileStream(req, metadata=self.metadata)

    def walk_file(self, commit, path):
        """
        Walks over all descendant files in a directory. Returns a generator of
        `FileInfo` objects.

        Params:

        * `commit`: A tuple, string, or `Commit` object representing the
        commit.
        * `path`: The path to the directory.
        """
        commit = commit_from(commit)
        f = pfs_proto.File(commit=commit_from(commit), path=path)
        req = pfs_proto.WalkFileRequest(file=f)
        return self._pfs_stub.WalkFile(req, metadata=self.metadata)

    def glob_file(self, commit, pattern):
        """
        Lists files that match a glob pattern. Yields `FileInfo` objects.

        Params:

        * `commit`: A tuple, string, or `Commit` object representing the
        commit.
        * `pattern`: A string representing a glob pattern.
        """

        req = pfs_proto.GlobFileRequest(commit=commit_from(commit), pattern=pattern)
        return self._pfs_stub.GlobFileStream(req, metadata=self.metadata)

    def delete_file(self, commit, path):
        """
        Deletes a file from a Commit. DeleteFile leaves a tombstone in the
        Commit, assuming the file isn't written to later attempting to get the
        file from the finished commit will result in not found error. The file
        will of course remain intact in the Commit's parent.

        Params:

        * `commit`: A tuple, string, or `Commit` object representing the
        commit.
        * `path`: The path to the file.
        """
        req = pfs_proto.DeleteFileRequest(file=pfs_proto.File(commit=commit_from(commit), path=path))
        self._pfs_stub.DeleteFile(req, metadata=self.metadata)

    def inspect_job(self, job_id, block_state=None, output_commit=None):
        """
        Inspects a job with a given ID. Returns a `JobInfo`.

        Params:

        * `job_id`: The ID of the job to inspect.
        * `block_state`: If true, block until the job completes.
        * `output_commit`: An optional tuple, string, or `Commit` object
        representing an output commit to filter on.
        """

        output_commit = commit_from(output_commit) if output_commit is not None else None
        req = pps_proto.InspectJobRequest(job=pps_proto.Job(id=job_id),
                                          block_state=block_state,
                                          output_commit=output_commit)
        return self._pps_stub.InspectJob(req, metadata=self.metadata)

    def list_job(self, pipeline_name=None, input_commit=None, output_commit=None, history=None):
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
        """

        pipeline = pps_proto.Pipeline(name=pipeline_name) if pipeline_name is not None else None

        if isinstance(input_commit, list):
            input_commit = [commit_from(ic) for ic in input_commit]
        elif input_commit is not None:
            input_commit = [commit_from(input_commit)]

        output_commit = commit_from(output_commit) if output_commit is not None else None

        req = pps_proto.ListJobRequest(pipeline=pipeline, input_commit=input_commit,
                                       output_commit=output_commit, history=history)

        return self._pps_stub.ListJobStream(req, metadata=self.metadata)

    def flush_job(self, commits, pipeline_names=None):
        """
        Blocks until all of the jobs which have a set of commits as
        provenance have finished. Yields `JobInfo` objects.

        Params:

        * `commits`: A list of tuples, strings, or `Commit` objects
        representing the commits to flush.
        * `pipeline_names`: An optional list of strings specifying pipeline
        names. If specified, only jobs within these pipelines will be flushed.
        """

        commits = [commit_from(c) for c in commits]
        pipelines = [pps_proto.Pipeline(name=name) for name in pipeline_names] if pipeline_names is not None else None
        req = pps_proto.FlushJobRequest(commits=commits, to_pipelines=pipelines)
        return self._pps_stub.FlushJob(req)

    def delete_job(self, job_id):
        """
        Deletes a job by its ID.

        Params:

        * `job_id`: The ID of the job to delete.
        """

        req = pps_proto.DeleteJobRequest(job=pps_proto.Job(id=job_id))
        self._pps_stub.DeleteJob(req, metadata=self.metadata)

    def stop_job(self, job_id):
        """
        Stops a job by its ID.

        Params:

        * `job_id`: The ID of the job to stop.
        """

        req = pps_proto.StopJobRequest(job=pps_proto.Job(id=job_id))
        self._pps_stub.StopJob(req, metadata=self.metadata)

    def inspect_datum(self, job_id, datum_id):
        """
        Inspects a datum. Returns a `DatumInfo` object.

        Params:

        * `job_id`: The ID of the job.
        * `datum_id`: The ID of the datum.
        """

        req = pps_proto.InspectDatumRequest(datum=pps_proto.Datum(id=datum_id, job=pps_proto.Job(id=job_id)))
        return self._pps_stub.InspectDatum(req, metadata=self.metadata)

    def list_datum(self, job_id, page_size=None, page=None):
        """
        Lists datums. Yields `ListDatumStreamResponse` objects.

        Params:

        * `job_id`: The ID of the job.
        * `page_size`: An optional int specifying the size of the page.
        * `page`: An optional int specifying the page number.
        """

        req = pps_proto.ListDatumRequest(job=pps_proto.Job(id=job_id), page_size=page_size, page=page)
        return self._pps_stub.ListDatumStream(req, metadata=self.metadata)

    def restart_datum(self, job_id, data_filters=None):
        """
        Restarts a datum.

        Params:

        * `job_id`: The ID of the job.
        * `data_filters`: An optional iterable of strings.
        """

        req = pps_proto.RestartDatumRequest(job=pps_proto.Job(id=job_id), data_filters=data_filters)
        self._pps_stub.RestartDatum(req, metadata=self.metadata)

    def create_pipeline(self, pipeline_name, transform=None, parallelism_spec=None,
                        hashtree_spec=None, egress=None, update=None, output_branch=None,
                        scale_down_threshold=None, resource_requests=None,
                        resource_limits=None, input=None, description=None, cache_size=None,
                        enable_stats=None, reprocess=None, max_queue_size=None,
                        service=None, chunk_spec=None, datum_timeout=None,
                        job_timeout=None, salt=None, standby=None, datum_tries=None,
                        scheduling_spec=None, pod_patch=None):
        """
        Creates a pipeline. For more info, please refer to the pipeline spec
        document:
        http://docs.pachyderm.io/en/latest/reference/pipeline_spec.html

        Params:

        * `pipeline_name`: A string representing the pipeline name.
        * `transform`: An optional `Transform` object.
        * `parallelism_spec`: An optional `ParallelismSpec` object.
        * `hashtree_spec`: An optional `HashtreeSpec` object.
        * `egress`: An optional `Egress` object.
        * `update`: An optional bool specifying whether this should behave as
        an upsert.
        * `output_branch`: An optional string representing the branch to output
        results on.
        * `scale_down_threshold`: An optional pps_proto.uf `Duration` object.
        * `resource_requests`: An optional `ResourceSpec` object.
        * `resource_limits`: An optional `ResourceSpec` object.
        * `input`: An optional `Input` object.
        * `description`: An optional string describing the pipeline.
        * `cache_size`: An optional string.
        * `enable_stats`: An optional bool.
        * `reprocess`: An optional bool. If true, pachyderm forces the pipeline
        to reprocess all datums. It only has meaning if `update` is `True`.
        * `max_queue_size`: An optional int.
        * `service`: An optional `Service` object.
        * `chunk_spec`: An optional `ChunkSpec` object.
        * `datum_timeout`: An optional pps_proto.uf `Duration` object.
        * `job_timeout`: An optional pps_proto.uf `Duration` object.
        * `salt`: An optional stirng.
        * `standby`: An optional bool.
        * `datum_tries`: An optional int.
        * `scheduling_spec`: An optional `SchedulingSpec` object.
        * `pod_patch`: An optional string.
        """

        req = pps_proto.CreatePipelineRequest(
            pipeline=pps_proto.Pipeline(name=pipeline_name),
            transform=transform, parallelism_spec=parallelism_spec,
            hashtree_spec=hashtree_spec, egress=egress, update=update,
            output_branch=output_branch, scale_down_threshold=scale_down_threshold,
            resource_requests=resource_requests, resource_limits=resource_limits,
            input=input, description=description, cache_size=cache_size,
            enable_stats=enable_stats, reprocess=reprocess,
            max_queue_size=max_queue_size, service=service,
            chunk_spec=chunk_spec, datum_timeout=datum_timeout,
            job_timeout=job_timeout, salt=salt, standby=standby,
            datum_tries=datum_tries, scheduling_spec=scheduling_spec,
            pod_patch=pod_patch
        )
        self._pps_stub.CreatePipeline(req, metadata=self.metadata)

    def inspect_pipeline(self, pipeline_name, history=None):
        """
        Inspects a pipeline. Returns a `PipelineInfo` object.

        Params:

        * `pipeline_name`: A string representing the pipeline name.
        * `history`: An optional int that indicates to return jobs from
        historical versions of pipelines. Semantics are:
            * 0: Return jobs from the current version of the pipeline or
              pipelines.
            * 1: Return the above and jobs from the next most recent version
            * 2: etc.
            * -1: Return jobs from all historical versions.
        """

        pipeline = pps_proto.Pipeline(name=pipeline_name)

        if history is None:
            req = pps_proto.InspectPipelineRequest(pipeline=pipeline)
            return self._pps_stub.InspectPipeline(req, metadata=self.metadata)
        else:
            # `InspectPipeline` doesn't support history, but `ListPipeline`
            # with a pipeline filter does, so we use that here
            req = pps_proto.ListPipelineRequest(pipeline=pipeline, history=history)
            pipelines = self._pps_stub.ListPipeline(req, metadata=self.metadata).pipeline_info
            assert len(pipelines) <= 1
            return pipelines[0] if len(pipelines) else None

    def list_pipeline(self, history=None):
        """
        Lists pipelines. Returns a `PipelineInfos` object.

        Params:

        * `pipeline_name`: A string representing the pipeline name.
        * `history`: An optional int that indicates to return jobs from
        historical versions of pipelines. Semantics are:
            * 0: Return jobs from the current version of the pipeline or
              pipelines.
            * 1: Return the above and jobs from the next most recent version
            * 2: etc.
            * -1: Return jobs from all historical versions.
        """
        req = pps_proto.ListPipelineRequest(history=history)
        return self._pps_stub.ListPipeline(req, metadata=self.metadata)

    def delete_pipeline(self, pipeline_name, force=None):
        """
        Deletes a pipeline.

        Params:

        * `pipeline_name`: A string representing the pipeline name.
        * `force`: Whether to force delete.
        """

        req = pps_proto.DeletePipelineRequest(pipeline=pps_proto.Pipeline(name=pipeline_name), force=force)
        self._pps_stub.DeletePipeline(req, metadata=self.metadata)

    def delete_all_pipelines(self, force=None):
        """
        Deletes all pipelines.

        Params:

        * `force`: Whether to force delete.
        """

        req = pps_proto.DeletePipelineRequest(all=True, force=force)
        self._pps_stub.DeletePipeline(req, metadata=self.metadata)

    def start_pipeline(self, pipeline_name):
        """
        Starts a pipeline.

        Params:

        * `pipeline_name`: A string representing the pipeline name.
        """

        req = pps_proto.StartPipelineRequest(pipeline=pps_proto.Pipeline(name=pipeline_name))
        self._pps_stub.StartPipeline(req, metadata=self.metadata)

    def stop_pipeline(self, pipeline_name):
        """
        Stops a pipeline.

        Params:

        * `pipeline_name`: A string representing the pipeline name.
        """
        req = pps_proto.StopPipelineRequest(pipeline=pps_proto.Pipeline(name=pipeline_name))
        self._pps_stub.StopPipeline(req, metadata=self.metadata)

    def run_pipeline(self, pipeline_name, provenance=None):
        """
        Runs a pipeline.

        Params:

        * `pipeline_name`: A string representing the pipeline name.
        * `provenance`: An optional iterable of `CommitProvenance` objects
        representing the pipeline execution provenance.
        """
        req = pps_proto.RunPipelineRequest(
            pipeline=pps_proto.Pipeline(name=pipeline_name),
            provenance=provenance,
        )
        self._pps_stub.RunPipeline(req, metadata=self.metadata)

    def delete_all(self):
        """
        Deletes everything in pachyderm.
        """
        req = pps_proto.google_dot_pps_proto.uf_dot_empty__pb2.Empty()
        self._pps_stub.DeleteAll(req, metadata=self.metadata)

    def get_pipeline_logs(self, pipeline_name, data_filters=None, master=None,
                          datum=None, follow=None, tail=None):
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
        """

        req = pps_proto.GetLogsRequest(
            pipeline=pps_proto.Pipeline(name=pipeline_name),
            data_filters=data_filters, master=master, datum=datum,
            follow=follow, tail=tail,
        )
        return self._pps_stub.GetLogs(req, metadata=self.metadata)

    def get_job_logs(self, job_id, data_filters=None, datum=None, follow=None,
                     tail=None):
        """
        Gets logs for a job. Yields `LogMessage` objects.

        Params:

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
        """

        req = pps_proto.GetLogsRequest(
            job=pps_proto.Job(id=job_id), data_filters=data_filters, datum=datum,
            follow=follow, tail=tail,
        )
        return self._pps_stub.GetLogs(req, metadata=self.metadata)

    def garbage_collect(self):
        """
        Runs garbage collection.
        """
        return self._pps_stub.GarbageCollect(pps_proto.GarbageCollectRequest(), metadata=self.metadata)
