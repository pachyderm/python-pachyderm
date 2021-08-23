import io
import itertools
import tarfile
from contextlib import contextmanager
from typing import Iterable, Iterator, Union, List

from python_pachyderm.pfs import commit_from, Commit, uuid_re
from python_pachyderm.service import pfs_proto, Service
from google.protobuf import empty_pb2, wrappers_pb2


BUFFER_SIZE = 19 * 1024 * 1024


class FileTarstream:
    """
    Implements a file-like interface over a GRPC byte stream,
    so we can use tarfile to decode the file contents.
    """

    def __init__(self, res):
        self.res = res
        self.buf = []

    def __next__(self):
        return next(self.res).value

    def close(self):
        self.res.cancel()

    def read(self, size=-1):
        if self.res.cancelled():
            return b""

        buf = []
        remaining = size if size >= 0 else 2 ** 32

        if self.buf:
            buf.append(self.buf[:remaining])
            self.buf = self.buf[remaining:]
            remaining -= len(buf[-1])

        try:
            while remaining > 0:
                b = next(self)

                if len(b) > remaining:
                    buf.append(b[:remaining])
                    self.buf = b[remaining:]
                else:
                    buf.append(b)

                remaining -= len(buf[-1])
        except StopIteration:
            pass

        return b"".join(buf)


class PFSFile:
    """
    The contents of a file stored in PFS. You can treat these as
    file-like objects, like so:

    ```
    source_file = client.get_file("montage/master", "/montage.png")
    with open("montage.png", "wb") as dest_file:
        shutil.copyfileobj(source_file, dest_file)
    ```
    """

    def __init__(self, stream, is_tar=False):
        if is_tar:
            # Pachyderm's GetFileTar API returns its result (which may include
            # several files, e.g. when getting a directory) as a tar
            # stream--untar the response byte stream as we receive it from
            # GetFileTar.
            # TODO how to handle multiple files in the tar stream?
            f = tarfile.open(fileobj=stream, mode="r|*")
            self._file = f.extractfile(f.next())
        else:
            self._file = stream

    def __iter__(self):
        return self

    def __next__(self):
        x = self.read()
        if not x:
            raise StopIteration
        return x

    def read(self, size=-1):
        return self._file.read(size)

    def close(self):
        self._file.close()


class PFSMixin:
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
        self._req(
            Service.PFS,
            "CreateRepo",
            repo=pfs_proto.Repo(name=repo_name, type="user"),
            description=description,
            update=update,
        )

    def inspect_repo(self, repo_name):
        """
        Returns info about a specific repo. Returns a `RepoInfo` object.

        Params:

        * `repo_name`: Name of the repo.
        """
        return self._req(
            Service.PFS, "InspectRepo", repo=pfs_proto.Repo(name=repo_name, type="user")
        )

    def list_repo(self, type=""):
        """
        Returns info about all repos, as a list of `RepoInfo` objects.

        Params:

        * `type`: the type of (system) repos that should be returned,
        an empty value None or empty string requests all repos.
        """
        return self._req(Service.PFS, "ListRepo", type=type)

    def delete_repo(self, repo_name, force=False):
        """
        Deletes a repo and reclaims the storage space it was using.

        Params:

        * `repo_name`: The name of the repo.
        * `force`: If set to true, the repo will be removed regardless of
          errors. This argument should be used with care.
        """
        self._req(
            Service.PFS,
            "DeleteRepo",
            repo=pfs_proto.Repo(name=repo_name, type="user"),
            force=force,
        )

    def delete_all_repos(self):
        """
        Deletes all repos.
        """
        self._req(Service.PFS, "DeleteAll", req=empty_pb2.Empty())

    def start_commit(self, repo_name, branch, parent=None, description=None) -> Commit:
        """
        Begins the process of committing data to a Repo. Once started you can
        write to the Commit with ModifyFile and when all the data has been
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
        """
        if parent and isinstance(parent, str):
            parent = pfs_proto.Commit(
                id=parent,
                branch=pfs_proto.Branch(
                    repo=pfs_proto.Repo(name=repo_name, type="user"), name=None
                ),
            )
        return self._req(
            Service.PFS,
            "StartCommit",
            parent=parent,
            branch=pfs_proto.Branch(
                repo=pfs_proto.Repo(name=repo_name, type="user"), name=branch
            ),
            description=description,
        )

    def finish_commit(self, commit, description=None, error=None, force=None):
        """
        Ends the process of committing data to a Repo and persists the
        Commit. Once a Commit is finished the data becomes immutable and
        future attempts to write to it with ModifyFile will error.

        Params:

        * `commit`: A tuple, string, or `Commit` object representing the
        commit.
        * `description`: An optional string describing this commit.
        * `error`: An optional bool. Used to mark that even though this
        commit is finished, it was interrupted or didn't occur properly.
        * `force`: An optional bool. If true, commit will be forcefully
        finished, even if it breaks provenance.
        """
        self._req(
            Service.PFS,
            "FinishCommit",
            commit=commit_from(commit),
            description=description,
            error=error,
            force=force,
        )

    @contextmanager
    def commit(self, repo_name, branch, parent=None, description=None):
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

    def inspect_commit(
        self,
        commit: Union[str, tuple, dict, Commit, pfs_proto.Commit],
        commit_state: pfs_proto.CommitState = None,
    ):
        """
        Inspects a commit. Yields `CommitInfo` objects.

        Params:

        * `commit`: A tuple, string, or `Commit` object representing the
        commit.
        * `commit_state`: An optional int that causes this method to block
        until the commit is in the desired state.
            1: STARTED
            2: READY
            3: FINISHING
            4: FINISHED
        """
        if not isinstance(commit, str):
            return iter(
                [
                    self._req(
                        Service.PFS,
                        "InspectCommit",
                        commit=commit_from(commit),
                        wait=commit_state,
                    )
                ]
            )
        elif uuid_re.match(commit):
            return self._req(
                Service.PFS,
                "InspectCommitSet",
                commit_set=pfs_proto.CommitSet(id=commit),
                wait=commit_state == pfs_proto.CommitState.FINISHED,
            )
        raise ValueError(
            "bad argument: commit should either be a commit ID (str) or a commit-like object"
        )

    def list_commit(
        self,
        repo_name=None,
        to_commit=None,
        from_commit=None,
        number=None,
        reverse=None,
        all=False,
        origin_kind: pfs_proto.OriginKind = 0,
    ):
        """
        Lists commits. Yields `CommitInfo` or `CommitSetInfo` objects.

        Params:

        * `repo_name`: Optional. If only `repo_name` is given, all commits
        in the repo are returned.
        * `to_commit`: Optional. Only the ancestors of `to`, including `to`
        itself, are considered.
        * `from_commit`: Optional. Only the descendants of `from`, including
        `from` itself, are considered.
        * `number`: Optional. Determines how many commits are returned.  If
        `number` is 0, all commits that match the aforementioned criteria are
        returned.
        * `reverse`: Optional. If true, commits are returned oldest to newest.
        * `all`: Optional. If true, all types of commits are returned.
        * `origin_kind`: Optional. Returns only commits of this enum type if
        `repo_name` is provided.
        """
        if repo_name is not None:
            req = pfs_proto.ListCommitRequest(
                repo=pfs_proto.Repo(name=repo_name, type="user"),
                number=number,
                reverse=reverse,
                all=all,
                origin_kind=origin_kind,
            )
            if to_commit is not None:
                req.to.CopyFrom(commit_from(to_commit))
            if from_commit is not None:
                getattr(req, "from").CopyFrom(commit_from(from_commit))
            return self._req(Service.PFS, "ListCommit", req=req)
        else:
            return self._req(Service.PFS, "ListCommitSet")

    def squash_commit(self, commit_id: str):
        """
        Squashes the subcommits of a `Commit` into their children.
        Params:
        * `commit_id`: the id of a `Commit`.
        """
        self._req(
            Service.PFS,
            "SquashCommitSet",
            commit_set=pfs_proto.CommitSet(id=commit_id),
        )

    def drop_commit(self, commit_id: str):
        """
        Drops an entire commit.

        Params:
        * `commit_id`: the id of a `Commit`.
        """
        self._req(
            Service.PFS,
            "DropCommitSet",
            commit_set=pfs_proto.CommitSet(id=commit_id),
        )

    def wait_commit(
        self, commit: Union[str, tuple, dict, Commit, pfs_proto.Commit]
    ) -> List[pfs_proto.CommitInfo]:
        """
        Waits for the specified commit or commit_set to finish and return them.

        Params:
        * `commit`: A `Commit` object, tuple, or str. If passed a commit_set_id,
           then wait for the entire commit_set.
        """
        return list(self.inspect_commit(commit, pfs_proto.CommitState.FINISHED))

    def subscribe_commit(
        self,
        repo_name,
        branch,
        from_commit_id=None,
        state: pfs_proto.CommitState = 1,
        all=False,
        origin_kind: pfs_proto.OriginKind = 0,
    ):
        """
        Yields `CommitInfo` objects as commits occur.

        Params:

        * `repo_name`: A string specifying the name of the repo.
        * `branch`: A string specifying branch to subscribe to.
        * `from_commit_id`: An optional string specifying the commit ID. Only
        commits created since this commit are returned.
        * `state`: The commit state to filter on.
        * `all`: Optional. If true, all types of commits are returned.
        * `origin_kind`: Optional. Returns only commits of this enum type.
        """
        repo = pfs_proto.Repo(name=repo_name, type="user")
        req = pfs_proto.SubscribeCommitRequest(
            repo=repo, branch=branch, state=state, all=all, origin_kind=origin_kind
        )
        if from_commit_id is not None:
            getattr(req, "from").CopyFrom(
                pfs_proto.Commit(repo=repo, id=from_commit_id)
            )
        return self._req(Service.PFS, "SubscribeCommit", req=req)

    def create_branch(
        self,
        repo_name,
        branch_name,
        head_commit=None,
        provenance=None,
        trigger=None,
        new_commit=False,
    ):
        """
        Creates a new branch.

        Params:

        * `repo_name`: A string specifying the name of the repo.
        * `branch_name`: A string specifying the new branch name.
        * `commit`: An optional tuple, string, or `Commit` object representing
           the head commit of the branch.
        * `provenance`: An optional iterable of `Branch` objects representing
          the branch provenance.
        * `trigger`: An optional `Trigger` object controlling when the head of
          `branch_name` is moved.
        * `new_commitset`: A bool, overrides the default behavior of using the
           same Commitset as 'head'
        """
        self._req(
            Service.PFS,
            "CreateBranch",
            branch=pfs_proto.Branch(
                repo=pfs_proto.Repo(name=repo_name, type="user"), name=branch_name
            ),
            head=commit_from(head_commit),
            provenance=provenance,
            trigger=trigger,
            new_commit_set=new_commit,
        )

    def inspect_branch(self, repo_name, branch_name):
        """
        Inspects a branch. Returns a `BranchInfo` object.
        """
        return self._req(
            Service.PFS,
            "InspectBranch",
            branch=pfs_proto.Branch(
                repo=pfs_proto.Repo(name=repo_name, type="user"), name=branch_name
            ),
        )

    def list_branch(self, repo_name, reverse=None):
        """
        Lists the active branch objects on a repo. Returns a list of
        `BranchInfo` objects.

        Params:

        * `repo_name`: A string specifying the repo name.
        * `reverse`: Optional. If true, returns branches oldest to newest.
        """
        return self._req(
            Service.PFS,
            "ListBranch",
            repo=pfs_proto.Repo(name=repo_name, type="user"),
            reverse=reverse,
        )

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
        self._req(
            Service.PFS,
            "DeleteBranch",
            branch=pfs_proto.Branch(
                repo=pfs_proto.Repo(name=repo_name, type="user"), name=branch_name
            ),
            force=force,
        )

    @contextmanager
    def modify_file_client(self, commit):
        """
        A context manager that gives a `ModifyFileClient`. When the context
        manager exits, any operations enqueued from the `ModifyFileClient` are
        executed in a single, atomic `ModifyFile` call.
        """
        pfc = ModifyFileClient(commit)
        yield pfc
        self._req(Service.PFS, "ModifyFile", req=pfc._reqs())

    def put_file_bytes(
        self,
        commit,
        path,
        value,
        datum=None,
        append=None,
    ):
        """
        Uploads a PFS file from a file-like object, bytestring, or iterator
        of bytestrings.

        Params:

        * `commit`: A tuple, string, or `Commit` object representing the
        commit.
        * `path`: A string specifying the path in the repo the file(s) will be
        written to.
        * `value`: The file contents as bytes, represented as a file-like
        object, bytestring, or iterator of bytestrings.
        * `datum`: A string for the file datum.
        * `append`: An optional bool, if true the data is appended to the file,
        if it already exists.
        """
        with self.modify_file_client(commit) as pfc:
            if hasattr(value, "read"):
                pfc.put_file_from_fileobj(
                    path,
                    value,
                    datum=datum,
                    append=append,
                )
            else:
                pfc.put_file_from_bytes(
                    path,
                    value,
                    datum=datum,
                    append=append,
                )

    def put_file_url(
        self,
        commit,
        path,
        url,
        recursive=None,
        datum=None,
        append=None,
    ):
        """
        Puts a file using the content found at a URL. The URL is sent to the
        server which performs the request.

        Params:

        * `commit`: A tuple, string, or `Commit` object representing the
        commit.
        * `path`: A string specifying the path to the file.
        * `url`: A string specifying the url of the file to put.
        * `recursive`: allow for recursive scraping of some types URLs, for
        example on s3:// URLs.
        * `datum`: A string for the file datum.
        * `append`: An optional bool, if true the data is appended to the file,
        if it already exists.
        """

        with self.modify_file_client(commit) as pfc:
            pfc.put_file_from_url(
                path,
                url,
                recursive=recursive,
                datum=datum,
                append=append,
            )

    def copy_file(
        self,
        source_commit,
        source_path,
        dest_commit,
        dest_path,
        datum=None,
        append=None,
    ):
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
        * `datum`: A string for the file datum.
        * `append`: An optional bool, if true the data is appended to the file,
        if it already exists.
        """
        with self.modify_file_client(dest_commit) as pfc:
            pfc.copy_file(
                source_commit, source_path, dest_path, datum=datum, append=append
            )

    def get_file(self, commit, path, datum=None, URL=None, offset=None):
        """
        Returns a `PFSFile` object, containing the contents of a file stored
        in PFS.

        Params:

        * `commit`: A tuple, dict, or `Commit` object representing the
        commit.
        * `path`: A string specifying the path of the file.
        * `datum`: A string to distinguish files by.
        * `URL`: A string that specifies an object storage URL that the file
        will be uploaded to.
        * `offset`: An int that allows file read to begin at `offset` number of
        bytes.
        """
        res = self._req(
            Service.PFS,
            "GetFile",
            file=pfs_proto.File(commit=commit_from(commit), path=path, datum=datum),
            URL=URL,
            offset=offset,
        )
        return PFSFile(io.BytesIO(next(res).value))

    def get_file_tar(self, commit, path, datum=None, URL=None, offset=None):
        """
        Returns a `PFSFile` object, containing the contents of a file stored
        in PFS.

        Params:

        * `commit`: A tuple, dict, or `Commit` object representing the
        commit.
        * `path`: A string specifying the path of the file.
        * `datum`: A string to distinguish files by.
        * `URL`: A string that specifies an object storage URL that the file
        will be uploaded to.
        * `offset`: An int that allows file read to begin at `offset` number of
        bytes.
        """
        res = self._req(
            Service.PFS,
            "GetFileTAR",
            req=pfs_proto.GetFileRequest(
                file=pfs_proto.File(commit=commit_from(commit), path=path, datum=datum),
                URL=URL,
                offset=offset,
            ),
        )
        return PFSFile(io.BytesIO(next(res).value), is_tar=True)

    def inspect_file(self, commit, path):
        """
        Inspects a file. Returns a `FileInfo` object.

        Params:

        * `commit`: A tuple, string, or `Commit` object representing the
        commit.
        * `path`: A string specifying the path to the file.
        """
        return self._req(
            Service.PFS,
            "InspectFile",
            file=pfs_proto.File(commit=commit_from(commit), path=path),
        )

    def list_file(self, commit, path):
        """
        Lists the files in a directory.

        Params:

        * `commit`: A tuple, string, or `Commit` object representing the
        commit.
        * `path`: The path to the directory.
        """
        return self._req(
            Service.PFS,
            "ListFile",
            file=pfs_proto.File(commit=commit_from(commit), path=path),
            # history=history,
        )

    def walk_file(self, commit, path):
        """
        Walks over all descendant files in a directory. Returns a generator of
        `FileInfo` objects.

        Params:

        * `commit`: A tuple, string, or `Commit` object representing the
        commit.
        * `path`: The path to the directory.
        """
        return self._req(
            Service.PFS,
            "WalkFile",
            file=pfs_proto.File(commit=commit_from(commit), path=path),
        )

    def glob_file(self, commit, pattern):
        """
        Lists files that match a glob pattern. Yields `FileInfo` objects.

        Params:

        * `commit`: A tuple, string, or `Commit` object representing the
        commit.
        * `pattern`: A string representing a glob pattern.
        """
        return self._req(
            Service.PFS, "GlobFile", commit=commit_from(commit), pattern=pattern
        )

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
        with self.modify_file_client(commit) as pfc:
            pfc.delete_file(path)

    def fsck(self, fix=None):
        """
        Performs a file system consistency check for PFS.
        """
        return self._req(Service.PFS, "Fsck", fix=fix)

    def diff_file(
        self, new_commit, new_path, old_commit=None, old_path=None, shallow=None
    ):
        """
        Diffs two files. If `old_commit` or `old_path` are not specified, the
        same path in the parent of the file specified by `new_commit` and
        `new_path` will be used.

        Params:

        * `new_commit`: A tuple, string, or `Commit` object representing the
        commit for the new file.
        * `new_path`: A string specifying the path of the new file.
        * `old_commit`: A tuple, string, or `Commit` object representing the
        commit for the old file.
        * `old_path`: A string specifying the path of the old file.
        * `shallow`: An optional bool specifying whether to do a shallow diff.
        """

        if old_commit is not None and old_path is not None:
            old_file = pfs_proto.File(commit=commit_from(old_commit), path=old_path)
        else:
            old_file = None

        return self._req(
            Service.PFS,
            "DiffFile",
            new_file=pfs_proto.File(commit=commit_from(new_commit), path=new_path),
            old_file=old_file,
            shallow=shallow,
        )


class ModifyFileClient:
    """
    `ModifyFileClient` puts or deletes PFS files atomically.
    """

    def __init__(self, commit):
        self._ops = []
        self.commit = commit_from(commit)

    def _reqs(self):
        yield pfs_proto.ModifyFileRequest(set_commit=self.commit)
        for op in self._ops:
            yield from op.reqs()

    def put_file_from_filepath(
        self,
        pfs_path,
        local_path,
        datum=None,
        append=None,
    ):
        """
        Uploads a PFS file from a local path at a specified path. This will
        lazily open files, which will prevent too many files from being
        opened, or too much memory being consumed, when atomically putting
        many files.

        Params:

        * `pfs_path`: A string specifying the path in the repo the file(s)
        will be written to.
        * `local_path`: A string specifying the local file path.
        * `append`: An optional bool, if true the data is appended to the file,
        if it already exists.
        * `datum`: A string for the file datum.
        """
        self._ops.append(
            AtomicModifyFilepathOp(
                pfs_path,
                local_path,
                datum,
                append,
            )
        )

    def put_file_from_fileobj(
        self,
        path,
        value,
        datum=None,
        append=None,
    ):
        """
        Uploads a PFS file from a file-like object.

        Params:

        * `path`: A string specifying the path in the repo the file(s) will be
        written to.
        * `value`: The file-like object.
        * `append`: An optional bool, if true the data is appended to the file,
        if it already exists.
        """
        self._ops.append(
            AtomicModifyFileobjOp(
                path,
                value,
                datum,
                append,
            )
        )

    def put_file_from_bytes(
        self,
        path,
        value,
        datum=None,
        append=None,
    ):
        """
        Uploads a PFS file from a bytestring.

        Params:

        * `path`: A string specifying the path in the repo the file(s) will be
        written to.
        * `value`: The file contents as a bytestring.
        * `datum`: A string for the file datum.
        * `append`: An optional bool, if true the data is appended to the file,
        if it already exists.
        """
        self.put_file_from_fileobj(
            path,
            io.BytesIO(value),
            datum=datum,
            append=append,
        )

    def put_file_from_url(
        self,
        path,
        url,
        datum=None,
        append=None,
        recursive=None,
    ):
        """
        Puts a file using the content found at a URL. The URL is sent to the
        server which performs the request.

        Params:

        * `path`: A string specifying the path to the file.
        * `url`: A string specifying the url of the file to put.
        * `datum`: A string for the file datum.
        * `append`: An optional bool, if true the data is appended to the file,
        if it already exists.
        * `recursive`: allow for recursive scraping of some types URLs, for
        example on s3:// URLs.
        """
        self._ops.append(
            AtomicModifyFileURLOp(
                path,
                url,
                datum=datum,
                append=append,
                recursive=recursive,
            )
        )

    def delete_file(self, path, datum=None):
        """
        Deletes a file.

        Params:

        * `path`: The path to the file.
        * `datum`: A string for the file datum.
        """
        self._ops.append(AtomicDeleteFileOp(path, datum=datum))

    def copy_file(
        self, source_commit, source_path, dest_path, datum=None, append=False
    ):
        """
        Copy a file.

        Params:

        * `source_commit`: The commit the source file is in.
        * `source_path`: The path to the source file.
        * `dest_path`: The path to the destination file.
        * `datum`: A string for the file datum.
        * `append`: An optional bool, if true the data is appended to the file,
        if it already exists.
        """
        self._ops.append(
            AtomicCopyFileOp(
                source_commit,
                source_path,
                dest_path,
                datum=datum,
                append=append,
            )
        )


class AtomicOp:
    """
    Represents an operation in a `ModifyFile` call.
    """

    def __init__(self, path, datum):
        self.path = path
        self.datum = datum

    def reqs(self):
        """
        Yields one or more protobuf `ModifyFileRequests`, which are then
        enqueued into the request's channel.
        """
        pass


class AtomicModifyFilepathOp(AtomicOp):
    """
    A `ModifyFile` operation to put a file locally stored at a given path. This
    file is opened on-demand, which helps with minimizing the number of open
    files.
    """

    def __init__(self, pfs_path, local_path, datum=None, append=False):
        super().__init__(pfs_path, datum)
        self.local_path = local_path
        self.append = append

    def reqs(self):
        if not self.append:
            yield delete_file_req(self.path, self.datum)
        with open(self.local_path, "rb") as f:
            yield add_file_req(path=self.path, datum=self.datum)
            for i, chunk in enumerate(f):
                yield add_file_req(path=self.path, datum=self.datum, chunk=chunk)


class AtomicModifyFileobjOp(AtomicOp):
    """A `ModifyFile` operation to put a file from a file-like object."""

    def __init__(self, path, fobj, datum=None, append=False):
        super().__init__(path, datum)
        self.fobj = fobj
        self.append = append

    def reqs(self):
        if not self.append:
            yield delete_file_req(self.path, self.datum)
        yield add_file_req(path=self.path, datum=self.datum)
        for i in itertools.count():
            chunk = self.fobj.read(BUFFER_SIZE)
            if len(chunk) == 0:
                return
            yield add_file_req(path=self.path, datum=self.datum, chunk=chunk)


class AtomicModifyFileURLOp(AtomicOp):
    """A `ModifyFile` operation to put a file from a URL."""

    def __init__(self, path, url, datum=None, append=False, recursive=False):
        super().__init__(path, datum)
        self.url = url
        self.recursive = recursive
        self.append = append

    def reqs(self):
        if not self.append:
            yield delete_file_req(self.path, self.datum)
        yield pfs_proto.ModifyFileRequest(
            add_file=pfs_proto.AddFile(
                path=self.path,
                datum=self.datum,
                url=pfs_proto.AddFile.URLSource(
                    URL=self.url,
                    recursive=self.recursive,
                ),
            ),
        )


class AtomicCopyFileOp(AtomicOp):
    """A `ModifyFile` operation to copy a file."""

    def __init__(self, source_commit, source_path, dest_path, datum=None, append=False):
        super().__init__(dest_path, datum)
        self.source_commit = commit_from(source_commit)
        self.source_path = source_path
        self.dest_path = dest_path
        self.append = append

    def reqs(self):
        yield pfs_proto.ModifyFileRequest(
            copy_file=pfs_proto.CopyFile(
                append=self.append,
                datum=self.datum,
                dst=self.dest_path,
                src=pfs_proto.File(commit=self.source_commit, path=self.source_path),
            ),
        )


class AtomicDeleteFileOp(AtomicOp):
    """A `ModifyFile` operation to delete a file."""

    def __init__(self, pfs_path, datum=None):
        super().__init__(pfs_path, datum)

    def reqs(self):
        yield delete_file_req(self.path, self.datum)


def add_file_req(path=None, datum=None, chunk=None):
    return pfs_proto.ModifyFileRequest(
        add_file=pfs_proto.AddFile(
            path=path, datum=datum, raw=wrappers_pb2.BytesValue(value=chunk)
        ),
    )


def delete_file_req(path=None, datum=None):
    return pfs_proto.ModifyFileRequest(
        delete_file=pfs_proto.DeleteFile(path=path, datum=datum)
    )
