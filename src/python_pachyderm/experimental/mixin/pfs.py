import io
import re
import os
import time
import itertools
import subprocess
from pathlib import Path
from contextlib import contextmanager
from typing import Iterator, Union, List, BinaryIO

import grpclib
from betterproto import BytesValue
from grpclib.exceptions import GRPCError

from python_pachyderm.mixin.pfs import (
    BUFFER_SIZE,
    PFSTarFile,
    PFSFile as _PFSFile,
    ModifyFileClient as _ModifyFileClientStable,
)
from ..pfs import SubcommitType, commit_from, uuid_re
from ..proto.v2.pfs_v2 import (
    ApiStub as _PFSApiStub,
    CreateRepoRequest,
    ModifyFileRequest,
    DiffFileResponse,
    FsckResponse,
    Branch,
    BranchInfo,
    Commit,
    CommitInfo,
    CommitSet,
    CommitSetInfo,
    CommitState,
    FileInfo,
    FileType,
    OriginKind,
    Repo,
    RepoInfo,
    Trigger,
    AddFile as _AddFile,
    AddFileUrlSource as _AddFileUrlSource,
    CopyFile as _CopyFile,
    DeleteFile as _DeleteFile,
    File as _File,
)
from . import _synchronizer

# bp_to_pb: bp_proto.Empty -> empty_pb2.Empty
# bp_to_pb: url -> URL (get_file_tar())


class _CancellableStream:
    """Wrap a grpclib stream to make it cancellable."""

    def __init__(self, stream: Iterator[BytesValue]):
        self._stream = stream
        self._active = True

    def __iter__(self) -> Iterator[BytesValue]:
        return self

    def __next__(self) -> BytesValue:
        if not self._active:
            raise StopIteration
        try:
            return next(self._stream)
        except StopIteration as stop:
            self._active = False
            raise stop

    def cancel(self) -> None:
        del self._stream
        self._stream = iter([])
        self._active = False

    def is_active(self) -> bool:
        return self._active


class ExperimentalPFSFile(_PFSFile):

    _Error = grpclib.GRPCError

    def __init__(self, stream: Iterator[BytesValue]):
        super().__init__(_CancellableStream(stream))


@_synchronizer
class PFSApi(_synchronizer(_PFSApiStub)):
    """A mixin with pfs-related functionality."""

    async def create_repo(
        self, repo_name: str, description: str = None, update: bool = False
    ) -> None:
        """Creates a new repo object in PFS with the given name. Repos are the
        top level data object in PFS and should be used to store data of a
        similar type. For example rather than having a single repo for an
        entire project you might have separate repos for logs, metrics,
        database dumps etc.

        Parameters
        ----------
        repo_name : str
            Name of the repo.
        description : str, optional
            Description of the repo.
        update : bool, optional
            Whether to update if the repo already exists.
        """
        repo = Repo(name=repo_name, type="user")
        await super().create_repo(repo=repo, description=description, update=update)

    async def inspect_repo(self, repo_name: str) -> RepoInfo:
        """Inspects a repo.

        Parameters
        ----------
        repo_name : str
            Name of the repo.

        Returns
        -------
        pfs_proto.RepoInfo
            A protobuf object with info on the repo.
        """
        repo = Repo(name=repo_name, type="user")
        return await super().inspect_repo(repo=repo)

    async def list_repo(self, type: str = "user") -> Iterator[RepoInfo]:
        """Lists all repos in PFS.

        Parameters
        ----------
        type : str, optional
            The type of repos that should be returned ("user", "meta", "spec").
            If unset, returns all types of repos.

        Returns
        -------
        Iterator[pfs_proto.RepoInfo]
            An iterator of protobuf objects that contain info on a repo.
        """
        async for repo in super().list_repo(type=type):
            yield repo

    async def delete_repo(self, repo_name: str, force: bool = False) -> None:
        """Deletes a repo and reclaims the storage space it was using.

        Parameters
        ----------
        repo_name : str
            The name of the repo.
        force : bool, optional
            If set to true, the repo will be removed regardless of errors.
            Use with care.
        """
        repo = Repo(name=repo_name, type="user")
        await super().delete_repo(repo=repo, force=force)

    async def delete_all_repos(self) -> None:
        """Deletes all repos."""
        await super().delete_all()

    async def start_commit(
        self,
        repo_name: str,
        branch: str,
        parent: Union[str, SubcommitType] = None,
        description: str = None,
    ) -> Commit:
        """Begins the process of committing data to a repo. Once started you
        can write to the commit with ModifyFile. When all the data has been
        written, you must finish the commit with FinishCommit. NOTE: data is
        not persisted until FinishCommit is called.

        Parameters
        ----------
        repo_name : str
            The name of the repo.
        branch_name : str
            A string specifying the branch.
        parent : Union[str, tuple, dict, Commit, pfs_proto.Commit], optional
            A commit specifying the parent of the newly created commit. Upon
            creation, before data is modified, the new commit will appear
            identical to the parent.
        description : str, optional
            A description of the commit.

        Returns
        -------
        pfs_proto.Commit
            A protobuf object that represents an open subcommit (commit at the
            repo-level).

        Examples
        --------
        >>> c = client.start_commit("foo", "master", ("foo", "staging"))
        """
        repo = Repo(name=repo_name, type="user")
        if parent and isinstance(parent, str):
            parent = Commit(id=parent, branch=Branch(repo=repo))
        return await super().start_commit(
            parent=commit_from(parent),
            branch=Branch(repo=repo, name=branch),
            description=description,
        )

    async def finish_commit(
        self,
        commit: SubcommitType,
        description: str = None,
        error: str = None,
        force: bool = False,
    ) -> None:
        """Ends the process of committing data to a repo and persists the
        commit. Once a commit is finished the data becomes immutable and
        future attempts to write to it with ModifyFile will error.

        Parameters
        ----------
        commit : Union[tuple, dict, Commit, pfs_proto.Commit]
            The subcommit (commit at the repo-level) object to close.
        description : str, optional
            A description of the commit. It will overwrite the description set
            in ``start_commit()``.
        error : str, optional
            If set, a message that errors out the commit. Don't use unless you
            want the finish commit request to fail.
        force : bool, optional
            If true, forces commit to finish, even if it breaks provenance.

        Examples
        --------
        Commit needs to be open still, either from the result of
        ``start_commit()`` or within scope of ``commit()``

        >>> client.start_commit("foo", "master")
        >>> # modify open commit
        >>> client.finish_commit(("foo", "master"))
        ...
        >>> # same as above
        >>> c = client.start_commit("foo", "master")
        >>> # modify open commit
        >>> client.finish_commit(c)
        """
        await super().finish_commit(
            commit=commit_from(commit),
            description=description,
            error=error,
            force=force,
        )

    @contextmanager
    def commit(
        self,
        repo_name: str,
        branch: str,
        parent: Union[str, SubcommitType] = None,
        description: str = None,
    ) -> Iterator[Commit]:
        """A context manager for running operations within a commit.

        Parameters
        ----------
        repo_name : str
            The name of the repo.
        branch_name : str
            A string specifying the branch.
        parent : Union[str, tuple, dict, Commit, pfs_proto.Commit], optional
            A commit specifying the parent of the newly created commit. Upon
            creation, before data is modified, the new commit will appear
            identical to the parent.
        description : str, optional
            A description of the commit.

        Yields
        -------
        pfs_proto.Commit
            A protobuf object that represents a commit.

        Examples
        --------
        >>> with client.commit("foo", "master") as c:
        >>>     client.delete_file(c, "/dir/delete_me.txt")
        >>>     client.put_file_bytes(c, "/new_file.txt", b"DATA")
        """
        commit = self.start_commit(repo_name, branch, parent, description)
        try:
            yield commit
        finally:
            self.finish_commit(commit)

    async def inspect_commit(
        self,
        commit: Union[str, SubcommitType],
        commit_state: CommitState = CommitState.STARTED,
    ) -> Iterator[CommitInfo]:
        """Inspects a commit.

        Parameters
        ----------
        commit : Union[str, tuple, dict, Commit, pfs_proto.Commit]
            The commit to inspect. Can either be a commit ID or a commit object
            that represents a subcommit (commit at the repo-level).
        commit_state : {pfs_proto.CommitState.STARTED, pfs_proto.CommitState.READY, pfs_proto.CommitState.FINISHING, pfs_proto.CommitState.FINISHED}, optional
            An enum that causes the method to block until the commit is in the
            specified state. (Default value = ``pfs_proto.CommitState.STARTED``)

        Returns
        -------
        Iterator[pfs_proto.CommitInfo]
            An iterator of protobuf objects that contain info on a subcommit
            (commit at the repo-level).

        Examples
        --------
        >>> # commit at repo-level
        >>> list(client.inspect_commit(("foo", "master~2")))
        ...
        >>> # an entire commit
        >>> for commit in client.inspect_commit("467c580611234cdb8cc9758c7aa96087", CommitState.FINISHED)
        >>>     print(commit)

        .. # noqa: W505
        """
        if not isinstance(commit, str):
            yield await super().inspect_commit(
                commit=commit_from(commit), wait=commit_state
            )

        elif uuid_re.match(commit):
            commit_set_response = super().inspect_commit_set(
                commit_set=CommitSet(id=commit),
                wait=(commit_state == CommitState.FINISHED),
            )
            async for commit_info in commit_set_response:
                yield commit_info
        else:
            raise ValueError(
                "bad argument: commit should either be a commit ID (str) or a commit-like object"
            )

    async def list_commit(
        self,
        repo_name: str = None,
        to_commit: SubcommitType = None,
        from_commit: SubcommitType = None,
        number: int = None,
        reverse: bool = False,
        all: bool = False,
        origin_kind: OriginKind = OriginKind.USER,
    ) -> Union[Iterator[CommitInfo], Iterator[CommitSetInfo]]:
        """Lists commits.

        Parameters
        ----------
        repo_name : str, optional
            The name of a repo. If set, returns subcommits (commit at
            repo-level) only in this repo.
        to_commit : Union[tuple, dict, Commit, pfs_proto.Commit], optional
            A subcommit (commit at repo-level) that only impacts results if
            `repo_name` is specified. If set, only the ancestors of
            `to_commit`, including `to_commit`, are returned.
        from_commit : Union[tuple, dict, Commit, pfs_proto.Commit], optional
            A subcommit (commit at repo-level) that only impacts results if
            `repo_name` is specified. If set, only the descendants of
            `from_commit`, including `from_commit`, are returned.
        number : int, optional
            The number of subcommits to return. If unset, all subcommits that
            matched the aforementioned criteria are returned. Only impacts
            results if `repo_name` is specified.
        reverse : bool, optional
            If true, returns the subcommits oldest to newest. Only impacts
            results if `repo_name` is specified.
        all : bool, optional
            If true, returns all types of subcommits. Otherwise, alias
            subcommits are excluded. Only impacts results if `repo_name` is
            specified.
        origin_kind : {pfs_proto.OriginKind.USER, pfs_proto.OriginKind.AUTO, pfs_proto.OriginKind.FSCK, pfs_proto.OriginKind.ALIAS}, optional
            An enum that specifies how a subcommit originated. Returns only
            subcommits of this enum type. Only impacts results if `repo_name`
            is specified.

        Returns
        -------
        Union[Iterator[pfs_proto.CommitInfo], Iterator[pfs_proto.CommitSetInfo]]
            An iterator of protobuf objects that either contain info on a
            subcommit (commit at the repo-level), if `repo_name` was specified,
            or a commit, if `repo_name` wasn't specified.

        Examples
        --------
        >>> # all commits at repo-level
        >>> for c in client.list_commit("foo"):
        >>>     print(c)
        ...
        >>> # all commits
        >>> commits = list(client.list_commit())

        .. # noqa: W505
        """
        if repo_name is not None:
            response = super().list_commit(
                repo=Repo(name=repo_name, type="user"),
                to=commit_from(to_commit),
                from_=commit_from(from_commit),
                number=number,
                reverse=reverse,
                all=all,
                origin_kind=origin_kind,
            )
            async for commit_info in response:
                yield commit_info
        else:
            async for commit_set_info in super().list_commit_set():
                yield commit_set_info

    async def squash_commit(self, commit_id: str) -> None:
        """Squashes a commit into its parent.

        Parameters
        ----------
        commit_id : str
            The ID of the commit.
        """
        await super().squash_commit_set(commit_set=CommitSet(id=commit_id))

    async def drop_commit(self, commit_id: str) -> None:
        """
        Drops an entire commit.

        Parameters
        ----------
        commit_id : str
            The ID of the commit.
        """
        await super().drop_commit_set(commit_set=CommitSet(id=commit_id))

    async def wait_commit(self, commit: Union[str, SubcommitType]) -> List[CommitInfo]:
        """Waits for the specified commit to finish.

        Parameters
        ----------
        commit : Union[str, tuple, dict, Commit, pfs_proto.Commit]
            A commit object to wait on. Can either be an entire commit or a
            subcommit (commit at the repo-level).

        Returns
        -------
        List[pfs_proto.CommitInfo]
            A list of protobuf objects that contain info on subcommits (commit
            at the repo-level). These are the individual subcommits this
            function waited on.

        Examples
        --------
        >>> # wait for an entire commit to finish
        >>> subcommits = client.wait_commit("467c580611234cdb8cc9758c7aa96087")
        ...
        >>> # wait for a commit to finish at a certain repo
        >>> client.wait_commit(("foo", "master"))
        """
        response = self.inspect_commit(commit, CommitState.FINISHED)
        return [commit_info async for commit_info in response]

    async def subscribe_commit(
        self,
        repo_name: str,
        branch: str,
        from_commit: Union[str, SubcommitType] = None,
        state: CommitState = CommitState.STARTED,
        all: bool = False,
        origin_kind: OriginKind = OriginKind.USER,
    ) -> Iterator[CommitInfo]:
        """Returns all commits on the branch and then listens for new commits
        that are created.

        Parameters
        ----------
        repo_name : str
            The name of the repo.
        branch : str
            The name of the branch.
        from_commit : Union[str, tuple, dict, Commit, pfs_proto.Commit], optional
            Return commits only from this commit and onwards. Can either be an
            entire commit or a subcommit (commit at the repo-level).
        state : {pfs_proto.CommitState.STARTED, pfs_proto.CommitState.READY, pfs_proto.CommitState.FINISHING, pfs_proto.CommitState.FINISHED}, optional
            Return commits only when they're at least in the specifed enum
            state. (Default value = ``pfs_proto.CommitState.STARTED``)
        all : bool, optional
            If true, returns all types of commits. Otherwise, alias commits are
            excluded.
        origin_kind : {pfs_proto.OriginKind.USER, pfs_proto.OriginKind.AUTO, pfs_proto.OriginKind.FSCK, pfs_proto.OriginKind.ALIAS}, optional
            An enum that specifies how a commit originated. Returns only
            commits of this enum type. (Default value = ``pfs_proto.OriginKind.USER``)

        Returns
        -------
        Iterator[pfs_proto.CommitInfo]
            An iterator of protobuf objects that contain info on subcommits
            (commits at the repo-level). Use ``next()`` to iterate through as
            the returned stream is potentially endless. Might block your code
            otherwise.

        Examples
        --------
        >>> commits = client.subscribe_commit("foo", "master", state=pfs_proto.CommitState.FINISHED)
        >>> c = next(commits)

        .. # noqa: W505
        """
        repo = Repo(name=repo_name, type="user")
        branch = Branch(repo=repo, name=branch)
        from_commit = (
            Commit(branch=branch, id=from_commit)
            if isinstance(from_commit, str)
            else commit_from(from_commit)
        )

        response = super().subscribe_commit(
            repo=repo,
            branch=branch.name,
            from_=from_commit,
            state=state,
            all=all,
            origin_kind=origin_kind,
        )
        async for commit_info in response:
            yield commit_info

    async def create_branch(
        self,
        repo_name: str,
        branch_name: str,
        head_commit: SubcommitType = None,
        provenance: List[Branch] = None,
        trigger: Trigger = None,
        new_commit: bool = False,
    ) -> None:
        """Creates a new branch.

        Parameters
        ----------
        repo_name : str
            The name of the repo.
        branch_name : str
            The name of the new branch.
        head_commit : Union[tuple, dict, Commit, pfs_proto.Commit], optional
            A subcommit (commit at repo-level) indicating the head of the
            new branch.
        provenance : List[pfs_proto.Branch], optional
            A list of branches to establish provenance with this newly created
            branch.
        trigger : pfs_proto.Trigger, optional
            Sets the conditions under which the head of this branch moves.
        new_commit : bool, optional
            If true and `head_commit` is specified, uses a different commit ID
            for head than `head_commit`.

        Examples
        --------
        >>> client.create_branch(
        ...     "bar",
        ...     "master",
        ...     provenance=[
        ...         Branch(repo=Repo(name="foo", type="user"), name="master")
        ...     ]
        ... )

        .. # noqa: W505
        """
        await super().create_branch(
            branch=Branch(repo=Repo(name=repo_name, type="user"), name=branch_name),
            head=commit_from(head_commit),
            provenance=provenance,
            trigger=trigger,
            new_commit_set=new_commit,
        )

    async def inspect_branch(self, repo_name: str, branch_name: str) -> BranchInfo:
        """Inspects a branch.

        Parameters
        ----------
        repo_name : str
            The name of the repo.
        branch_name : str
            The name of the branch.

        Returns
        -------
        pfs_proto.BranchInfo
            A protobuf object with info on a branch.
        """
        branch = Branch(repo=Repo(name=repo_name, type="user"), name=branch_name)
        return await super().inspect_branch(branch=branch)

    async def list_branch(
        self, repo_name: str, reverse: bool = False
    ) -> Iterator[BranchInfo]:
        """Lists the active branch objects in a repo.

        Parameters
        ----------
        repo_name : str
            The name of the repo.
        reverse : bool, optional
            If true, returns branches oldest to newest.

        Returns
        -------
        Iterator[pfs_proto.BranchInfo]
            An iterator of protobuf objects that contain info on a branch.

        Examples
        --------
        >>> branches = list(client.list_branch("foo"))
        """
        repo = Repo(name=repo_name, type="user")
        response = super().list_branch(repo=repo, reverse=reverse)
        async for branch_info in response:
            yield branch_info

    async def delete_branch(
        self, repo_name: str, branch_name: str, force: bool = False
    ) -> None:
        """Deletes a branch, but leaves the commits themselves intact. In other
        words, those commits can still be accessed via commit IDs and other
        branches they happen to be on.

        Parameters
        ----------
        repo_name : str
            The name of the repo.
        branch_name : str
            The name of the branch.
        force : bool, optional
            If true, forces the branch deletion.
        """
        branch = Branch(repo=Repo(name=repo_name, type="user"), name=branch_name)
        await super().delete_branch(branch=branch, force=force)

    @_synchronizer.asynccontextmanager
    async def modify_file_client(
        self, commit: SubcommitType
    ) -> Iterator["ModifyFileClient"]:
        """A context manager that gives a :class:`.ModifyFileClient`. When the
        context manager exits, any operations enqueued from the
        :class:`.ModifyFileClient` are executed in a single, atomic
        ModifyFile gRPC call.

        Parameters
        ----------
        commit : Union[tuple, dict, Commit, pfs_proto.Commit]
            A subcommit (commit at the repo-level) to modify. If this subcommit
            is opened before ``modify_file_client()`` is called, it will remain
            open after. If ``modify_file_client()`` opens the subcommit, it
            will close when exiting the ``with`` scope.

        Yields
        -------
        ModifyFileClient
            An object that can queue operations to modify a commit atomically.

        Examples
        --------
        On an open subcommit:

        >>> c = client.start_commit("foo", "master")
        >>> with client.modify_file_client(c) as mfc:
        >>>     mfc.delete_file("/delete_me.txt")
        >>>     mfc.put_file_from_url(
        ...         "/new_file.txt",
        ...         "https://example.com/data/train/input.txt"
        ...     )
        >>> client.finish_commit(c)

        Opening a subcommit:

        >>> with client.modify_file_client(("foo", "master")) as mfc:
        >>>     mfc.delete_file("/delete_me.txt")
        >>>     mfc.put_file_from_url(
        ...         "/new_file.txt",
        ...         "https://example.com/data/train/input.txt"
        ...     )
        """
        mfc = ModifyFileClient(commit)
        yield mfc
        await super().modify_file(mfc._reqs())

    def put_file_bytes(
        self,
        commit: SubcommitType,
        path: str,
        value: Union[bytes, BinaryIO],
        datum: str = None,
        append: bool = False,
    ) -> None:
        """Uploads a PFS file from a file-like object, bytestring, or iterator
        of bytestrings.

        Parameters
        ----------
        commit : Union[tuple, dict, Commit, pfs_proto.Commit]
            An open subcommit (commit at the repo-level) to modify.
        path : str
            The path in the repo the file(s) will be written to.
        value : Union[bytes, BinaryIO]
            The file contents as bytes, represented as a file-like object,
            bytestring, or iterator of bystrings.
        datum : str, optional
            A tag for the added file(s).
        append : bool, optional
            If true, appends the data to the file(s) specified at `path`, if
            they already exist. Otherwise, overwrites them.

        Examples
        --------
        Commit needs to be open still, either from the result of
        ``start_commit()`` or within scope of ``commit()``

        >>> with client.commit("foo", "master") as c:
        >>>     client.put_file_bytes(c, "/file.txt", b"SOME BYTES")
        """
        with self.modify_file_client(commit) as mfc:
            if hasattr(value, "read"):
                mfc.put_file_from_fileobj(
                    path,
                    value,
                    datum=datum,
                    append=append,
                )
            else:
                mfc.put_file_from_bytes(
                    path,
                    value,
                    datum=datum,
                    append=append,
                )

    def put_file_url(
        self,
        commit: SubcommitType,
        path: str,
        url: str,
        recursive: bool = False,
        datum: str = None,
        append: bool = False,
    ) -> None:
        """Uploads a PFS file using the content found at a URL. The URL is sent
        to the server which performs the request.

        Parameters
        ----------
        commit : Union[tuple, dict, Commit, pfs_proto.Commit]
            An open subcommit (commit at the repo-level) to modify.
        path : str
            The path in the repo the file(s) will be written to.
        url : str
            The URL of the file to put.
        recursive : bool, optional
            If true, allows for recursive scraping on some types URLs, for
            example on s3:// URLs
        datum : str, optional
            A tag for the added file(s).
        append : bool, optional
            If true, appends the data to the file(s) specified at `path`, if
            they already exist. Otherwise, overwrites them.

        Examples
        --------
        Commit needs to be open still, either from the result of
        ``start_commit()`` or within scope of ``commit()``

        >>> with client.commit("foo", "master") as c:
        >>>     client.put_file_url(
        ...         c,
        ...         "/file.txt",
        ...         "https://example.com/data/train/input.txt"
        ...     )
        """
        with self.modify_file_client(commit) as mfc:
            mfc.put_file_from_url(
                path,
                url,
                recursive=recursive,
                datum=datum,
                append=append,
            )

    def copy_file(
        self,
        source_commit: SubcommitType,
        source_path: str,
        dest_commit: SubcommitType,
        dest_path: str,
        datum: str = None,
        append: bool = False,
    ) -> None:
        """Efficiently copies files already in PFS. Note that the destination
        repo cannot be an output repo, or the copy operation will silently
        fail.

        Parameters
        ----------
        source_commit : Union[tuple, dict, Commit, pfs_proto.Commit]
            The subcommit (commit at the repo-level) which holds the source
            file.
        source_path : str
            The path of the source file.
        dest_commit : Union[tuple, dict, Commit, pfs_proto.Commit]
            The open subcommit (commit at the repo-level) to which to add the
            file.
        dest_path : str
            The path of the destination file.
        datum : str, optional
            A tag for the added file.
        append : bool, optional
            If true, appends the content of `source_path` to the file at
            `dest_path`, if it already exists. Otherwise, overwrites the file.

        Examples
        --------
        Destination commit needs to be open still, either from the result of
        ``start_commit()`` or within scope of ``commit()``

        >>> with client.commit("bar", "master") as c:
        >>>     client.copy_file(("foo", "master"), "/src/file.txt", c, "/file.txt")

        .. # noqa: W505
        """
        with self.modify_file_client(dest_commit) as mfc:
            mfc.copy_file(
                source_commit, source_path, dest_path, datum=datum, append=append
            )

    def get_file(
        self,
        commit: SubcommitType,
        path: str,
        datum: str = None,
        URL: str = None,
        offset: int = 0,
    ) -> ExperimentalPFSFile:
        """Gets a file from PFS.

        Parameters
        ----------
        commit : Union[tuple, dict, Commit, pfs_proto.Commit]
            The subcommit (commit at the repo-level) to get the file from.
        path : str
            The path of the file.
        datum : str, optional
            A tag that filters the files.
        URL : str, optional
            Specifies an object storage URL that the file will be uploaded to.
        offset : int, optional
            Allows file read to begin at `offset` number of bytes.

        Returns
        -------
        PFSFile
            The contents of the file in a file-like object.
        """
        file = _File(commit=commit_from(commit), path=path, datum=datum)
        stream = super().get_file(file=file, url=URL, offset=offset)
        return ExperimentalPFSFile(stream)

    def get_file_tar(
        self,
        commit: SubcommitType,
        path: str,
        datum: str = None,
        URL: str = None,
        offset: int = 0,
    ) -> PFSTarFile:
        """Gets a file from PFS.

        Parameters
        ----------
        commit : Union[tuple, dict, Commit, pfs_proto.Commit]
            The subcommit (commit at the repo-level) to get the file from.
        path : str
            The path of the file.
        datum : str, optional
            A tag that filters the files.
        URL : str, optional
            Specifies an object storage URL that the file will be uploaded to.
        offset : int, optional
            Allows file read to begin at `offset` number of bytes.

        Returns
        -------
        PFSFile
            The contents of the file in a file-like object.
        """
        file = _File(commit=commit_from(commit), path=path, datum=datum)
        stream = super().get_file_tar(file=file, url=URL, offset=offset)
        return PFSTarFile.open(fileobj=ExperimentalPFSFile(stream), mode="r|*")

    async def inspect_file(
        self,
        commit: SubcommitType,
        path: str,
        datum: str = None,
    ) -> FileInfo:
        """Inspects a file.

        Parameters
        ----------
        commit : Union[tuple, dict, Commit, pfs_proto.Commit]
            The subcommit (commit at the repo-level) to inspect the file from.
        path : str
            The path of the file.
        datum : str, optional
            A tag that filters the files.

        Returns
        -------
        pfs_proto.FileInfo
            A protobuf object that contains info on a file.
        """
        file = _File(commit=commit_from(commit), path=path, datum=datum)
        return await super().inspect_file(file=file)

    async def list_file(
        self,
        commit: SubcommitType,
        path: str,
        datum: str = None,
        details: bool = False,
    ) -> Iterator[FileInfo]:
        """Lists the files in a directory.

        Parameters
        ----------
        commit : Union[tuple, dict, Commit, pfs_proto.Commit]
            The subcommit (commit at the repo-level) to list files from.
        path : str
            The path to the directory.
        datum : str, optional
            A tag that filters the files.
        details : bool, optional
            Unused.

        Returns
        -------
        Iterator[pfs_proto.FileInfo]
            An iterator of protobuf objects that contain info on files.

        Examples
        --------
        >>> files = list(client.list_file(("foo", "master"), "/dir/subdir/"))
        """
        file = _File(commit=commit_from(commit), path=path, datum=datum)
        async for file_info in super().list_file(file=file):
            yield file_info

    async def walk_file(
        self,
        commit: SubcommitType,
        path: str,
        datum: str = None,
    ) -> Iterator[FileInfo]:
        """Walks over all descendant files in a directory.

        Parameters
        ----------
        commit : Union[tuple, dict, Commit, pfs_proto.Commit]
            The subcommit (commit at the repo-level) to walk files in.
        path : str
            The path to the directory.
        datum : str, optional
            A tag that filters the files.

        Returns
        -------
        Iterator[pfs_proto.FileInfo]
            An iterator of protobuf objects that contain info on files.

        Examples
        --------
        >>> files = list(client.walk_file(("foo", "master"), "/dir/subdir/"))
        """
        file = _File(commit=commit_from(commit), path=path, datum=datum)
        async for file_info in super().walk_file(file=file):
            yield file_info

    async def glob_file(
        self, commit: SubcommitType, pattern: str
    ) -> Iterator[FileInfo]:
        """Lists files that match a glob pattern.

        Parameters
        ----------
        commit : Union[tuple, dict, Commit, pfs_proto.Commit]
            The subcommit (commit at the repo-level) to query against.
        pattern : str
            A glob pattern.

        Returns
        -------
        Iterator[pfs_proto.FileInfo]
            An iterator of protobuf objects that contain info on files.

        Examples
        --------
        >>> files = list(client.glob_file(("foo", "master"), "/*.txt"))
        """
        response = super().glob_file(commit=commit_from(commit), pattern=pattern)
        async for file_info in response:
            yield file_info

    def delete_file(self, commit: SubcommitType, path: str) -> None:
        """Deletes a file from an open commit. This leaves a tombstone in the
        commit, assuming the file isn't written to later while the commit is
        still open. Attempting to get the file from the finished commit will
        result in a not found error. The file will of course remain intact in
        the commit's parent.

        Parameters
        ----------
        commit : Union[tuple, dict, Commit, pfs_proto.Commit]
            The open subcommit (commit at the repo-level) to delete a file
            from.
        path : str
            The path to the file.

        Examples
        --------
        Commit needs to be open still, either from the result of
        ``start_commit()`` or within scope of ``commit()``

        >>> with client.commit("bar", "master") as c:
        >>>     client.delete_file(c, "/delete_me.txt")
        """
        with self.modify_file_client(commit) as mfc:
            mfc.delete_file(path)

    async def fsck(self, fix: bool = False) -> Iterator[FsckResponse]:
        """Performs a file system consistency check on PFS, ensuring the
        correct provenance relationships are satisfied.

        Parameters
        ----------
        fix : bool, optional
            If true, attempts to fix as many problems as possible.

        Returns
        -------
        Iterator[pfs_proto.FsckResponse]
            An iterator of protobuf objects that contain info on either what
            error was encountered (and was unable to be fixed, if `fix` is set
            to ``True``) or a fix message (if `fix` is set to ``True``).

        Examples
        --------
        >>> for action in client.fsck(True):
        >>>     print(action)
        """
        async for fsck in super().fsck(fix=fix):
            yield fsck

    async def diff_file(
        self,
        new_commit: SubcommitType,
        new_path: str,
        old_commit: SubcommitType = None,
        old_path: str = None,
        shallow: bool = False,
    ) -> Iterator[DiffFileResponse]:
        """Diffs two PFS files (file = commit + path in Pachyderm) and returns
        files that are different. Similar to ``git diff``.

        If `old_commit` or `old_path` are not specified, `old_commit` will be
        set to the parent of `new_commit` and `old_path` will be set to
        `new_path`.

        Parameters
        ----------
        new_commit : Union[tuple, dict, Commit, pfs_proto.Commit]
            The newer subcommit (commit at the repo-level).
        new_path : str
            The path in `new_commit` to compare with.
        old_commit : Union[tuple, dict, Commit, pfs_proto.Commit], optional
            The older subcommit (commit at the repo-level).
        old_path : str, optional
            The path in `old_commit` to compare with.
        shallow : bool, optional
            Unused.

        Returns
        -------
        Iterator[pfs_proto.DiffFileResponse]
            An iterator of protobuf objects that contain info on files whose
            content has changed between commits. If a file under one of the
            paths is only in one commit, than the ``DiffFileResponse`` for it
            will only have one ``FileInfo`` set.

        Examples
        --------
        >>> # Compare files
        >>> res = client.diff_file(
        ...     ("foo", "master"),
        ...     "/a/file.txt",
        ...     ("foo", "master~2"),
        ...     "/a/file.txt"
        ... )
        >>> diff = next(res)
        ...
        >>> # Compare files in directories
        >>> res = client.diff_file(
        ...     ("foo", "master"),
        ...     "/a/",
        ...     ("foo", "master~2"),
        ...     "/a/"
        ... )
        >>> diff = next(res)
        """
        new_file = _File(commit=commit_from(new_commit), path=new_path)
        if old_commit is not None and old_path is not None:
            old_file = _File(commit=commit_from(old_commit), path=old_path)
        else:
            old_file = None

        response = super().diff_file(
            new_file=new_file, old_file=old_file, shallow=shallow
        )
        async for diff_file in response:
            yield diff_file

    def path_exists(self, commit: SubcommitType, path: str) -> bool:
        """Checks whether the path exists in the specified commit, agnostic to
        whether `path` is a file or a directory.

        Parameters
        ----------
        commit : SubcommitType
            The subcommit (commit at the repo-level) to check in.
        path : str
            The file or directory path in `commit`.

        Returns
        -------
        bool
            Returns ``True`` if `path` exists in `commit`. Otherwise, returns
            ``False``.
        """
        try:
            self.inspect_file(commit, path)
        except GRPCError as e:
            valid_commit_re = re.compile("^file .+ not found in repo .+ at commit .+$")
            invalid_commit_re = re.compile("^branch .+ not found in repo .+$")

            if valid_commit_re.match(e.message):
                return False
            elif invalid_commit_re.match(e.message):
                raise ValueError("bad argument: nonexistent commit provided")
            raise e

        return True

    def mount(self, mount_dir: str, repos: List[str] = None) -> None:
        """Mounts Pachyderm repos locally.

        Parameters
        ----------
        mount_dir : str
            The directory to mount repos to. Make sure if this folder already
            exists that it's empty (including hidden files).
        repos : List[str], optional
            The repos to mount. Each repo can only be mounted once, even if
            multiple branches are passed. If empty, all repos are mounted.

        Notes
        -----
        Mounting uses FUSE, which causes some known issues on macOS. For the
        best experience, we recommend using mount on Linux. We do not support
        mounting on macOS 1.11 and later.

        Additionally, we recommend using mount in read-only access.

        Examples
        --------
        >>> client.mount("dir_a", ["repo1", "repo2@staging"])
        """
        if repos is None:
            repos = []
        Path(mount_dir).mkdir(parents=True, exist_ok=True)

        subprocess.run(
            ["pachctl", "unmount", mount_dir],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )

        # Check for non-empty mount dir
        mount_dir_contents = next(os.walk(mount_dir))
        if mount_dir_contents[1] or mount_dir_contents[2]:
            raise RuntimeError(
                f"{mount_dir} must be empty to mount (including hidden files)"
            )

        # If 0 Pachyderm repos, no need to mount
        if not list(self.list_repo()):
            print("no repos in Pachyderm to mount")
            return

        cmd = ["pachctl", "mount", mount_dir]
        for r in repos:
            cmd.append("-r")
            cmd.append(r)

        subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)

        # Ensure mount has finished
        for _ in range(3):
            time.sleep(0.25)
            mounted_repos = next(os.walk(mount_dir))[1]

            if mounted_repos:
                return

        self.unmount(mount_dir)
        raise RuntimeError(
            "mount failed to expose data after three read attempts (0.75s)"
        )

    def unmount(self, mount_dir: str = None, *, all_mounts: bool = False) -> None:
        """Unmounts mounted local filesystems with Pachyderm repos.

        Parameters
        ----------
        mount_dir : str, optional
            The mounted directory to unmount.
        all_mounts : bool, optional
            If ``True``, unmounts all mounted directories.

        Examples
        --------
        >>> client.unmount("dir_a")
        ...
        >>> client.unmount(all_mounts=True)
        """
        if mount_dir is not None:
            subprocess.run(["pachctl", "unmount", mount_dir])
        elif all_mounts:
            subprocess.run(["pachctl", "unmount", "-a"], input=b"y\n")
        else:
            print("no repos unmounted, pass arguments or see documentation")


class ModifyFileClient(_ModifyFileClientStable):
    """:class:`.ModifyFileClient` puts or deletes PFS files atomically.
    Replaces :class:`.PutFileClient` from python_pachyderm 6.x.
    """

    def __init__(self, commit: SubcommitType):
        self._ops = []
        self.commit = commit_from(commit)

    def _reqs(self) -> Iterator[ModifyFileRequest]:
        yield ModifyFileRequest(set_commit=self.commit)
        for op in self._ops:
            yield from op.reqs()

    def put_file_from_filepath(
        self,
        pfs_path: str,
        local_path: str,
        datum: str = None,
        append: bool = False,
    ) -> None:
        """Uploads a PFS file from a local path at a specified path. This will
        lazily open files, which will prevent too many files from being
        opened, or too much memory being consumed, when atomically putting
        many files.

        Parameters
        ----------
        pfs_path : str
            The path in the repo the file will be written to.
        local_path : str
            The local file path.
        datum : str, optional
            A tag for the added file.
        append : bool, optional
            If true, appends the content of `local_path` to the file at
            `pfs_path`, if it already exists. Otherwise, overwrites the file.
        """
        self._ops.append(
            _AtomicModifyFilepathOp(
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
        """Uploads a PFS file from a file-like object.

        Parameters
        ----------
        path : str
            The path in the repo the file will be written to.
        value : BinaryIO
            The file-like object.
        datum : str, optional
            A tag for the added file.
        append : bool, optional
            If true, appends the content of `value` to the file at `path`,
            if it already exists. Otherwise, overwrites the file.
        """
        self._ops.append(
            _AtomicModifyFileobjOp(
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
        """Uploads a PFS file from a bytestring.

        Parameters
        ----------
        path : str
            The path in the repo the file will be written to.
        value : BinaryIO
            The file-like object.
        datum : str, optional
            A tag for the added file.
        append : bool, optional
            If true, appends the content of `value` to the file at `path`,
            if it already exists. Otherwise, overwrites the file.
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
        """Uploads a PFS File from the content found at a URL. The URL is
        sent to the server which performs the request.

        Parameters
        ----------
        path : str
            The path in the repo the file will be written to.
        url : str
            The URL of the file to upload.
        datum : str, optional
            A tag for the added file.
        append : bool, optional
            If true, appends the content to the file at `path`, if it
            already exists. Otherwise, overwrites the file.
        recursive : bool, optional
            If true, allows for recursive scraping on some types URLs, for
            example on s3:// URLs
        """
        self._ops.append(
            _AtomicModifyFileURLOp(
                path,
                url,
                datum=datum,
                append=append,
                recursive=recursive,
            )
        )

    def delete_file(self, path: str, datum: str = None) -> None:
        """Deletes a file.

        Parameters
        ----------
        path : str
            The path to the file.
        datum : str, optional
            A tag that filters the files.
        """
        self._ops.append(_AtomicDeleteFileOp(path, datum=datum))

    def copy_file(
        self,
        source_commit: SubcommitType,
        source_path: str,
        dest_path: str,
        datum: str = None,
        append: bool = False,
    ) -> None:
        """Copy a file.

        Parameters
        ----------
        source_commit : Union[tuple, dict, Commit, pfs_proto.Commit]
            The commit the source file is in.
        source_path : str
            The path to the source file.
        dest_path : str
            The path to the destination file.
        datum : str, optional
            A tag for the added file.
        append : bool, optional
            If true, appends the content of the source file to the
            destination file, if it already exists. Otherwise, overwrites
            the file.
        """
        self._ops.append(
            _AtomicCopyFileOp(
                source_commit,
                source_path,
                dest_path,
                datum=datum,
                append=append,
            )
        )


class _AtomicOp:
    """Represents an operation in a `ModifyFile` call."""

    def __init__(self, path: str, datum: str):
        self.path = path
        self.datum = datum

    def reqs(self):
        """Yields one or more protobuf `ModifyFileRequests`, which are then
        enqueued into the request's channel.
        """
        pass


class _AtomicModifyFilepathOp(_AtomicOp):
    """A `ModifyFile` operation to put a file locally stored at a given path.
    This file is opened on-demand, which helps with minimizing the number of
    open files.
    """

    def __init__(
        self, pfs_path: str, local_path: str, datum: str = None, append: bool = False
    ):
        super().__init__(pfs_path, datum)
        self.local_path = local_path
        self.append = append

    def reqs(self) -> Iterator[ModifyFileRequest]:
        if not self.append:
            yield _delete_file_req(self.path, self.datum)
        with open(self.local_path, "rb") as f:
            yield _add_file_req(path=self.path, datum=self.datum)
            for _, chunk in enumerate(f):
                yield _add_file_req(path=self.path, datum=self.datum, chunk=chunk)


class _AtomicModifyFileobjOp(_AtomicOp):
    """A `ModifyFile` operation to put a file from a file-like object."""

    def __init__(
        self, path: str, fobj: BinaryIO, datum: str = None, append: bool = False
    ):
        super().__init__(path, datum)
        self.fobj = fobj
        self.append = append

    def reqs(self) -> Iterator[ModifyFileRequest]:
        if not self.append:
            yield _delete_file_req(self.path, self.datum)
        yield _add_file_req(path=self.path, datum=self.datum)
        for _ in itertools.count():
            chunk = self.fobj.read(BUFFER_SIZE)
            if len(chunk) == 0:
                return
            yield _add_file_req(path=self.path, datum=self.datum, chunk=chunk)


class _AtomicModifyFileURLOp(_AtomicOp):
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

    def reqs(self) -> Iterator[ModifyFileRequest]:
        if not self.append:
            yield _delete_file_req(self.path, self.datum)
        yield ModifyFileRequest(
            add_file=_AddFile(
                path=self.path,
                datum=self.datum,
                url=_AddFileUrlSource(
                    url=self.url,
                    recursive=self.recursive,
                ),
            ),
        )


class _AtomicCopyFileOp(_AtomicOp):
    """A `ModifyFile` operation to copy a file."""

    def __init__(
        self,
        source_commit: SubcommitType,
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

    def reqs(self) -> Iterator[ModifyFileRequest]:
        yield ModifyFileRequest(
            copy_file=_CopyFile(
                append=self.append,
                datum=self.datum,
                dst=self.dest_path,
                src=_File(commit=self.source_commit, path=self.source_path),
            ),
        )


class _AtomicDeleteFileOp(_AtomicOp):
    """A `ModifyFile` operation to delete a file."""

    def __init__(self, pfs_path: str, datum: str = None):
        super().__init__(pfs_path, datum)

    def reqs(self):
        yield _delete_file_req(self.path, self.datum)


def _add_file_req(path: str, datum: str = None, chunk: bytes = None):
    return ModifyFileRequest(
        add_file=_AddFile(
            path=path,
            datum=datum,
            raw=chunk,
        ),
    )


def _delete_file_req(path: str, datum: str = None):
    return ModifyFileRequest(delete_file=_DeleteFile(path=path, datum=datum))
