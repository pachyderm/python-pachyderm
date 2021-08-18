import io
import itertools
import tarfile
from contextlib import contextmanager
from typing import Iterator, Union, List, BinaryIO

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

    def read(self, size=-1) -> bytes:
        return self._file.read(size)

    def close(self):
        self._file.close()


class PFSMixin:
    def create_repo(
        self, repo_name: str, description: str = None, update: bool = False
    ) -> None:
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

    def inspect_repo(self, repo_name: str) -> pfs_proto.RepoInfo:
        """
        Returns info about a specific repo. Returns a `RepoInfo` object.

        Params:

        * `repo_name`: Name of the repo.
        """
        return self._req(
            Service.PFS, "InspectRepo", repo=pfs_proto.Repo(name=repo_name, type="user")
        )

    def list_repo(self, type: str = None) -> Iterator[pfs_proto.RepoInfo]:
        """
        Returns info about all repos, as a list of `RepoInfo` objects.

        Params:

        * `type`: the type of (system) repos that should be returned,
        an empty value None or empty string requests all repos.
        """
        return self._req(Service.PFS, "ListRepo", type=type)

    def delete_repo(self, repo_name: str, force: bool = False) -> None:
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

    def delete_all_repos(self) -> None:
        """
        Deletes all repos.
        """
        self._req(Service.PFS, "DeleteAll", req=empty_pb2.Empty())

    def start_commit(
        self,
        repo_name: str,
        branch_name: str,
        parent_commit: Union[str, tuple, dict, Commit, pfs_proto.Commit] = None,
        description: str = None,
    ) -> pfs_proto.Commit:
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
        if parent_commit and isinstance(parent_commit, str):
            parent_commit = pfs_proto.Commit(
                id=parent_commit,
                branch=pfs_proto.Branch(
                    repo=pfs_proto.Repo(name=repo_name, type="user"), name=None
                ),
            )
        return self._req(
            Service.PFS,
            "StartCommit",
            parent=commit_from(parent_commit),
            branch=pfs_proto.Branch(
                repo=pfs_proto.Repo(name=repo_name, type="user"), name=branch_name
            ),
            description=description,
        )

    def finish_commit(
        self,
        commit: Union[tuple, dict, Commit, pfs_proto.Commit],
        description: str = None,
        error: str = None,
        force: bool = False,
    ) -> None:
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
    def commit(
        self,
        repo_name: str,
        branch_name: str,
        parent_commit: Union[str, tuple, dict, Commit, pfs_proto.Commit] = None,
        description: str = None,
    ) -> Iterator[pfs_proto.Commit]:
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
        commit = self.start_commit(repo_name, branch_name, parent_commit, description)
        try:
            yield commit
        finally:
            self.finish_commit(commit)

    def inspect_commit(
        self,
        commit: Union[tuple, dict, Commit, pfs_proto.Commit],
        commit_state: pfs_proto.CommitState = pfs_proto.CommitState.STARTED,
    ) -> pfs_proto.CommitInfo:
        """
        Inspects a commit. Returns a `CommitInfo` object.

        Params:

        * `commit`: A tuple, string, or `Commit` object representing the
        commit.
        * `commit_state`: An optional int that causes this method to block
        until the commit is in the desired state.
            0: STARTED
            1: READY
            2: FINISHED
        """
        return self._req(
            Service.PFS,
            "InspectCommit",
            commit=commit_from(commit),
            wait=commit_state,
        )

    def inspect_commit_set(
        self, commit_set_id: str, wait: bool = False
    ) -> Iterator[pfs_proto.CommitInfo]:
        """
        Inspects a commit set and returns an iterator of `CommitInfo` objects.

        * `commit_set_id`: the ID that represents this commit_set
        * `wait`: if true then wait until all commits in the set are finished.
        """
        return self._req(
            Service.PFS,
            "InspectCommitSet",
            commit_set=pfs_proto.CommitSet(id=commit_set_id),
            wait=wait,
        )

    def list_commit(
        self,
        repo_name: str,
        to_commit: Union[tuple, dict, Commit, pfs_proto.Commit] = None,
        from_commit: Union[tuple, dict, Commit, pfs_proto.Commit] = None,
        number: int = 0,
        reverse: bool = False,
        all: bool = False,
        origin_kind: pfs_proto.OriginKind = pfs_proto.OriginKind.USER,
    ) -> Iterator[pfs_proto.CommitInfo]:
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
        * `reverse`: Optional. If true, commits are returned oldest to newest.
        * `all`: Optional. If true, all types of commits are returned.
        * `origin_kind`: Optional. Returns only commits of this enum type.
        """
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

    def list_commit_set(self) -> Iterator[pfs_proto.CommitSetInfo]:
        """
        Lists all commits. Yields `CommitSetInfo` objects.
        """
        return self._req(
            Service.PFS,
            "ListCommitSet",
        )

    def squash_commit_set(self, commit_set_id: str) -> None:
        """
        Squashes the commits of a `CommitSet` into their children.
        Params:
        * `commit_set_id`: the id shared by all commits that form a transaction.
        commit.
        """
        self._req(
            Service.PFS,
            "SquashCommitSet",
            commit_set=pfs_proto.CommitSet(id=commit_set_id),
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
        if isinstance(commit, str) and uuid_re.match(commit):
            return list(self.inspect_commit_set(commit, True))
        return [self.inspect_commit(commit, pfs_proto.CommitState.FINISHED)]

    def subscribe_commit(
        self,
        repo_name: str,
        branch_name: str,
        from_commit: Union[str, tuple, dict, Commit, pfs_proto.Commit] = None,
        commit_state: pfs_proto.CommitState = pfs_proto.CommitState.STARTED,
        all: bool = False,
        origin_kind: pfs_proto.OriginKind = pfs_proto.OriginKind.USER,
    ) -> Iterator[pfs_proto.CommitInfo]:
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
        # TODO make it clear func requires next()
        repo = pfs_proto.Repo(name=repo_name, type="user")
        req = pfs_proto.SubscribeCommitRequest(
            repo=repo,
            branch=branch_name,
            state=commit_state,
            all=all,
            origin_kind=origin_kind,
        )
        if from_commit is not None:
            if isinstance(from_commit, str):
                getattr(req, "from").CopyFrom(
                    pfs_proto.Commit(repo=repo, id=from_commit)
                )
            else:
                getattr(req, "from").CopyFrom(commit_from(from_commit))
        return self._req(Service.PFS, "SubscribeCommit", req=req)

    def create_branch(
        self,
        repo_name: str,
        branch_name: str,
        commit: Union[tuple, dict, Commit, pfs_proto.Commit] = None,
        provenance: List[pfs_proto.Branch] = None,
        trigger: pfs_proto.Trigger = None,
        new_commit_set: bool = False,
    ) -> None:
        """
        Creates a new branch.

        Params:

        * `repo_name`: A string specifying the name of the repo.
        * `branch_name`: A string specifying the new branch name.
        * `commit`: An optional tuple, string, or `Commit` object representing
           the head commit of the branch.
        * `provenance`: An optional list of `Branch` objects representing
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
            head=commit_from(commit),
            provenance=provenance,
            trigger=trigger,
            new_commit_set=new_commit_set,
        )

    def inspect_branch(self, repo_name: str, branch_name: str) -> pfs_proto.BranchInfo:
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

    def list_branch(
        self, repo_name: str, reverse: bool = False
    ) -> Iterator[pfs_proto.BranchInfo]:
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

    def delete_branch(
        self, repo_name: str, branch_name: str, force: bool = False
    ) -> None:
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
    def modify_file_client(
        self, commit: Union[tuple, dict, Commit, pfs_proto.Commit]
    ) -> Iterator["ModifyFileClient"]:
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
        commit: Union[tuple, dict, Commit, pfs_proto.Commit],
        path: str,
        value: Union[bytes, BinaryIO],
        datum: str = None,
        append: bool = False,
    ) -> None:
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
        * `tag`: A string for the file tag.
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
        commit: Union[tuple, dict, Commit, pfs_proto.Commit],
        path: str,
        url: str,
        recursive: bool = False,
        datum: str = None,
        append: bool = False,
    ) -> None:
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
        * `tag`: A string for the file tag.
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
        source_commit: Union[tuple, dict, Commit, pfs_proto.Commit],
        source_path: str,
        dest_commit: Union[tuple, dict, Commit, pfs_proto.Commit],
        dest_path: str,
        datum: str = None,
        append: bool = False,
    ) -> None:
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
        * `tag`: A string for the file tag.
        * `append`: An optional bool, if true the data is appended to the file,
        if it already exists.
        """
        with self.modify_file_client(dest_commit) as pfc:
            pfc.copy_file(
                source_commit, source_path, dest_path, datum=datum, append=append
            )

    def get_file(
        self,
        commit: Union[tuple, dict, Commit, pfs_proto.Commit],
        path: str,
        datum: str = None,
        URL: str = None,
    ) -> PFSFile:
        """
        Returns a `PFSFile` object, containing the contents of a file stored
        in PFS.

        Params:

        * `commit`: A tuple, dict, or `Commit` object representing the
        commit.
        * `path`: A string specifying the path of the file.
        * `tag`: A string to distinguish files by.
        * `URL`: A string that specifies an object storage URL that the file
        will be uploaded to.
        """
        res = self._req(
            Service.PFS,
            "GetFile",
            file=pfs_proto.File(commit=commit_from(commit), path=path, datum=datum),
            URL=URL,
        )
        return PFSFile(io.BytesIO(next(res).value))

    def get_file_tar(
        self,
        commit: Union[tuple, dict, Commit, pfs_proto.Commit],
        path: str,
        datum: str = None,
        URL: str = None,
    ) -> PFSFile:
        """
        Returns a `PFSFile` object, containing the contents of a file stored
        in PFS.

        Params:

        * `commit`: A tuple, dict, or `Commit` object representing the
        commit.
        * `path`: A string specifying the path of the file.
        * `tag`: A string to distinguish files by.
        * `URL`: A string that specifies an object storage URL that the file
        will be uploaded to.
        """
        res = self._req(
            Service.PFS,
            "GetFileTAR",
            req=pfs_proto.GetFileRequest(
                file=pfs_proto.File(commit=commit_from(commit), path=path, datum=datum),
                URL=URL,
            ),
        )
        return PFSFile(io.BytesIO(next(res).value), is_tar=True)

    def inspect_file(
        self,
        commit: Union[tuple, dict, Commit, pfs_proto.Commit],
        path: str,
        datum: str = None,
    ) -> pfs_proto.FileInfo:
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
            file=pfs_proto.File(commit=commit_from(commit), path=path, datum=datum),
        )

    def list_file(
        self,
        commit: Union[tuple, dict, Commit, pfs_proto.Commit],
        path: str,
        datum: str = None,
        details: bool = False,
    ) -> Iterator[pfs_proto.FileInfo]:
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
            file=pfs_proto.File(commit=commit_from(commit), path=path, datum=datum),
            details=details,
        )

    def walk_file(
        self,
        commit: Union[tuple, dict, Commit, pfs_proto.Commit],
        path: str,
        datum: str = None,
    ) -> Iterator[pfs_proto.FileInfo]:
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
            file=pfs_proto.File(commit=commit_from(commit), path=path, datum=datum),
        )

    def glob_file(
        self, commit: Union[tuple, dict, Commit, pfs_proto.Commit], pattern: str
    ) -> Iterator[pfs_proto.FileInfo]:
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

    def delete_file(
        self, commit: Union[tuple, dict, Commit, pfs_proto.Commit], path: str
    ) -> None:
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

    def fsck(self, fix: bool = False) -> Iterator[pfs_proto.FsckResponse]:
        """
        Performs a file system consistency check for PFS.
        """
        return self._req(Service.PFS, "Fsck", fix=fix)

    def diff_file(
        self,
        new_commit: Union[tuple, dict, Commit, pfs_proto.Commit],
        new_path: str,
        old_commit: Union[tuple, dict, Commit, pfs_proto.Commit] = None,
        old_path: str = None,
        shallow: bool = False,
    ) -> Iterator[pfs_proto.DiffFileResponse]:
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

    def __init__(self, commit: Union[tuple, dict, Commit, pfs_proto.Commit]):
        self._ops = []
        self.commit = commit_from(commit)

    def _reqs(self) -> Iterator[pfs_proto.ModifyFileRequest]:
        yield pfs_proto.ModifyFileRequest(set_commit=self.commit)
        for op in self._ops:
            yield from op.reqs()

    def put_file_from_filepath(
        self,
        pfs_path: str,
        local_path: str,
        datum: str = None,
        append: bool = False,
    ) -> None:
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
        * `tag`: A string for the file tag.
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
        path: str,
        value: BinaryIO,
        datum: str = None,
        append: bool = False,
    ) -> None:
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
        path: str,
        value: bytes,
        datum: str = None,
        append: bool = False,
    ) -> None:
        """
        Uploads a PFS file from a bytestring.

        Params:

        * `path`: A string specifying the path in the repo the file(s) will be
        written to.
        * `value`: The file contents as a bytestring.
        * `tag`: A string for the file tag.
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
        path: str,
        url: str,
        datum: str = None,
        append: bool = False,
        recursive: bool = False,
    ) -> None:
        """
        Puts a file using the content found at a URL. The URL is sent to the
        server which performs the request.

        Params:

        * `path`: A string specifying the path to the file.
        * `url`: A string specifying the url of the file to put.
        * `tag`: A string for the file tag.
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

    def delete_file(self, path: str, datum: str = None) -> None:
        """
        Deletes a file.

        Params:

        * `path`: The path to the file.
        * `tag`: A string for the file tag.
        """
        self._ops.append(AtomicDeleteFileOp(path, datum=datum))

    def copy_file(
        self,
        source_commit: Union[tuple, dict, Commit, pfs_proto.Commit],
        source_path: str,
        dest_path: str,
        datum: str = None,
        append: bool = False,
    ) -> None:
        """
        Copy a file.

        Params:

        * `source_commit`: The commit the source file is in.
        * `source_path`: The path to the source file.
        * `dest_path`: The path to the destination file.
        * `tag`: A string for the file tag.
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

    def __init__(self, path: str, datum: str):
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

    def __init__(
        self, pfs_path: str, local_path: str, datum: str = None, append: bool = False
    ):
        super().__init__(pfs_path, datum)
        self.local_path = local_path
        self.append = append

    def reqs(self) -> Iterator[pfs_proto.ModifyFileRequest]:
        if not self.append:
            yield delete_file_req(self.path, self.datum)
        with open(self.local_path, "rb") as f:
            yield add_file_req(path=self.path, datum=self.datum)
            for _, chunk in enumerate(f):
                yield add_file_req(path=self.path, datum=self.datum, chunk=chunk)


class AtomicModifyFileobjOp(AtomicOp):
    """A `ModifyFile` operation to put a file from a file-like object."""

    def __init__(
        self, path: str, fobj: BinaryIO, datum: str = None, append: bool = False
    ):
        super().__init__(path, datum)
        self.fobj = fobj
        self.append = append

    def reqs(self) -> Iterator[pfs_proto.ModifyFileRequest]:
        if not self.append:
            yield delete_file_req(self.path, self.datum)
        yield add_file_req(path=self.path, datum=self.datum)
        for _ in itertools.count():
            chunk = self.fobj.read(BUFFER_SIZE)
            if len(chunk) == 0:
                return
            yield add_file_req(path=self.path, datum=self.datum, chunk=chunk)


class AtomicModifyFileURLOp(AtomicOp):
    """A `ModifyFile` operation to put a file from a URL."""

    def __init__(
        self,
        path: str,
        url: str,
        datum: str = None,
        append: bool = False,
        recursive: bool = False,
    ):
        super().__init__(path, datum)
        self.url = url
        self.recursive = recursive
        self.append = append

    def reqs(self) -> Iterator[pfs_proto.ModifyFileRequest]:
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

    def __init__(
        self,
        source_commit: Union[tuple, dict, Commit, pfs_proto.Commit],
        source_path: str,
        dest_path: str,
        datum: str = None,
        append: bool = False,
    ):
        super().__init__(dest_path, datum)
        self.source_commit = commit_from(source_commit)
        self.source_path = source_path
        self.dest_path = dest_path
        self.append = append

    def reqs(self) -> Iterator[pfs_proto.ModifyFileRequest]:
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

    def __init__(self, pfs_path: str, datum: str = None):
        super().__init__(pfs_path, datum)

    def reqs(self):
        yield delete_file_req(self.path, self.datum)


def add_file_req(path: str, datum: str = None, chunk: bytes = None):
    return pfs_proto.ModifyFileRequest(
        add_file=pfs_proto.AddFile(
            path=path, datum=datum, raw=wrappers_pb2.BytesValue(value=chunk)
        ),
    )


def delete_file_req(path: str, datum: str = None):
    return pfs_proto.ModifyFileRequest(
        delete_file=pfs_proto.DeleteFile(path=path, datum=datum)
    )
