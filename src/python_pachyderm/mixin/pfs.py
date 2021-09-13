import io
import warnings
import itertools
import collections
from contextlib import contextmanager

from python_pachyderm.proto.pfs import pfs_pb2 as pfs_proto
from python_pachyderm.service import Service
from .util import commit_from


BUFFER_SIZE = 19 * 1024 * 1024


class PFSFile:
    """The contents of a file stored in PFS.

    Examples
    --------
    You can treat these as either file-like objects, like so:

    >>> source_file = client.get_file("montage/master", "/montage.png")
    >>> with open("montage.png", "wb") as dest_file:
    >>>     shutil.copyfileobj(source_file, dest_file)

    Or as an iterator of bytes, like so:

    >>> source_file = client.get_file("montage/master", "/montage.png")
    >>> with open("montage.png", "wb") as dest_file:
    >>>     for chunk in source_file:
    >>>         dest_file.write(chunk)
    """

    def __init__(self, res):
        self.res = res
        self.buf = []

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.res).value

    def close(self):
        """Closes the :class:`.PFSFile`"""
        self.res.cancel()

    def read(self, size=-1):
        """Reads from the :class:`.PFSFile` buffer.

        Parameters
        ----------
        size : int, optional
            The number of bytes to read from the buffer.
        """
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


class PFSMixin:
    def create_repo(self, repo_name, description=None, update=None):
        """Creates a new ``Repo`` object in PFS with the given name. Repos are
        the top level data object in PFS and should be used to store data of a
        similar type. For example rather than having a single ``Repo`` for an
        entire project you might have separate ``Repo``s for logs, metrics,
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
        return self._req(
            Service.PFS,
            "CreateRepo",
            repo=pfs_proto.Repo(name=repo_name),
            description=description,
            update=update,
        )

    def inspect_repo(self, repo_name):
        """Returns info about a specific repo. Returns a ``RepoInfo`` object.

        Parameters
        ----------
        repo_name : str
            Name of the repo.
        """
        return self._req(
            Service.PFS, "InspectRepo", repo=pfs_proto.Repo(name=repo_name)
        )

    def list_repo(self):
        """Returns info about all repos, as a list of ``RepoInfo`` objects."""
        return self._req(Service.PFS, "ListRepo").repo_info

    def delete_repo(self, repo_name, force=None, split_transaction=None):
        """Deletes a repo and reclaims the storage space it was using.

        Parameters
        ----------
        repo_name : str
            The name of the repo.
        force : bool, optional
            If set to true, the repo will be removed regardless of errors. This
            argument should be used with care.
        split_transaction : bool, optional
            Controls whether Pachyderm attempts to delete the entire repo in a
            single database transaction. Setting this to ``True`` can work
            around certain Pachyderm errors, but, if set, the ``delete_repo()``
            call may need to be retried.
        """
        return self._req(
            Service.PFS,
            "DeleteRepo",
            repo=pfs_proto.Repo(name=repo_name),
            force=force,
            all=False,
            split_transaction=split_transaction,
        )

    def delete_all_repos(self, force=None):
        """Deletes all repos.

        Parameters
        ----------
        force : bool, optional
            If set to true, the repo will be removed regardless of errors. This
            argument should be used with care.
        """
        return self._req(Service.PFS, "DeleteRepo", force=force, all=True)

    def start_commit(
        self, repo_name, branch=None, parent=None, description=None, provenance=None
    ):
        """Begins the process of committing data to a Repo. Once started you
        can write to the Commit with PutFile and when all the data has been
        written you must finish the Commit with FinishCommit. NOTE, data is
        not persisted until FinishCommit is called. A Commit object is
        returned.

        Parameters
        ----------
        repo_name : str
            The name of the repo.
        branch : str, optional
            The branch name. This is a more convenient way to build linear
            chains of commits. When a commit is started with a non-empty
            branch the value of branch becomes an alias for the created Commit.
            This enables a more intuitive access pattern. When the commit is
            started on a branch the previous head of the branch is used as the
            parent of the commit.
        parent : Union[tuple, str, Commit probotuf], optional
            An optional ``Commit`` object specifying the parent commit. Upon
            creation the new commit will appear identical to the parent commit,
            data can safely be added to the new commit without affecting the
            contents of the parent commit.
        description : str, optional
            Description of the commit.
        provenance : List[CommitProvenance protobuf], optional
            An optional iterable of `CommitProvenance` objects specifying the
            commit provenance.
        """
        return self._req(
            Service.PFS,
            "StartCommit",
            parent=pfs_proto.Commit(repo=pfs_proto.Repo(name=repo_name), id=parent),
            branch=branch,
            description=description,
            provenance=provenance,
        )

    def finish_commit(
        self,
        commit,
        description=None,
        input_tree_object_hash=None,
        tree_object_hashes=None,
        datum_object_hash=None,
        size_bytes=None,
        empty=None,
    ):
        """Ends the process of committing data to a Repo and persists the
        Commit. Once a Commit is finished the data becomes immutable and
        future attempts to write to it with PutFile will error.

        Parameters
        ----------
        commit : Union[tuple, str, Commit protobuf]
            Represents the commit.
        description : str, optional
            Description of this commit.
        input_tree_object_hash : str, optional
            Specifies an input tree object hash.
        tree_object_hashes : List[str], optional
            A list of zero or more strings specifying object hashes for the
            output trees.
        datum_object_hash : str, optional
            Specifies an object hash.
        size_bytes : int, optional
            An optional int.
        empty : bool, optional
            If set, the commit will be closed (its `finished` field will be set
            to the current time) but its `tree` will be left None.
        """
        return self._req(
            Service.PFS,
            "FinishCommit",
            commit=commit_from(commit),
            description=description,
            tree=pfs_proto.Object(hash=input_tree_object_hash)
            if input_tree_object_hash is not None
            else None,
            trees=[pfs_proto.Object(hash=h) for h in tree_object_hashes]
            if tree_object_hashes is not None
            else None,
            datums=pfs_proto.Object(hash=datum_object_hash)
            if datum_object_hash is not None
            else None,
            size_bytes=size_bytes,
            empty=empty,
        )

    @contextmanager
    def commit(self, repo_name, branch=None, parent=None, description=None):
        """A context manager for running operations within a commit.

        Parameters
        ----------
        repo_name : str
            The name of the repo.
        branch : str, optional
            The branch name. This is a more convenient way to build linear
            chains of commits. When a commit is started with a non-empty
            branch the value of branch becomes an alias for the created Commit.
            This enables a more intuitive access pattern. When the commit is
            started on a branch the previous head of the branch is used as the
            parent of the commit.
        parent : Union[tuple, str, Commit probotuf], optional
            An optional ``Commit`` object specifying the parent commit. Upon
            creation the new commit will appear identical to the parent commit,
            data can safely be added to the new commit without affecting the
            contents of the parent commit.
        description : str, optional
            Description of the commit.
        """
        commit = self.start_commit(repo_name, branch, parent, description)
        try:
            yield commit
        finally:
            self.finish_commit(commit)

    def inspect_commit(self, commit, block_state=None):
        """Inspects a commit. Returns a ``CommitInfo`` object.

        Parameters
        ----------
        commit : Union[tuple, str, Commit protobuf]
            Represents the commit.
        block_state : int, optional
            Causes this method to block until the commit is in the desired
            commit state. See the ``CommitState`` enum.
        """
        return self._req(
            Service.PFS,
            "InspectCommit",
            commit=commit_from(commit),
            block_state=block_state,
        )

    def list_commit(
        self, repo_name, to_commit=None, from_commit=None, number=None, reverse=None
    ):
        """Lists commits. Yields ``CommitInfo`` objects.

        Parameters
        ----------
        repo_name : str
            If only `repo_name` is given, all commits in the repo are returned.
        to_commit : Union[tuple, str, Commit protobuf], optional
            Only the ancestors of `to`, including `to` itself, are considered.
        from_commit : Union[tuple, str, Commit protobuf], optional
            Only the descendants of `from`, including `from` itself, are
            considered.
        number : int, optional
            Determines how many commits are returned. If `number` is 0, all
            commits that match the aforementioned criteria are returned.
        reverse : bool, optional
            If true, returns commits oldest to newest.
        """
        req = pfs_proto.ListCommitRequest(
            repo=pfs_proto.Repo(name=repo_name), number=number, reverse=reverse
        )
        if to_commit is not None:
            req.to.CopyFrom(commit_from(to_commit))
        if from_commit is not None:
            getattr(req, "from").CopyFrom(commit_from(from_commit))
        return self._req(Service.PFS, "ListCommitStream", req=req)

    def delete_commit(self, commit):
        """Deletes a commit.

        Parameters
        ----------
        commit : Union[tuple, str, Commit protobuf]
            The commit to delete.
        """
        return self._req(Service.PFS, "DeleteCommit", commit=commit_from(commit))

    def flush_commit(self, commits, repos=None):
        """Blocks until all of the commits which have a set of commits as
        provenance have finished. For commits to be considered they must have
        all of the specified commits as provenance. This in effect waits for
        all of the jobs that are triggered by a set of commits to complete.
        It returns an error if any of the commits it's waiting on are
        cancelled due to one of the jobs encountering an error during runtime.
        Note that it's never necessary to call FlushCommit to run jobs,
        they'll run no matter what, FlushCommit just allows you to wait for
        them to complete and see their output once they do. This returns an
        iterator of CommitInfo objects.

        Yields ``CommitInfo`` objects.

        Parameters
        ----------
        commits : List[Union[tuple, str, Commit protobuf]]
            The commits to flush.
        repos : List[str], optional
            An optional list of strings specifying repo names. If specified,
            only commits within these repos will be flushed.
        """
        return self._req(
            Service.PFS,
            "FlushCommit",
            commits=[commit_from(c) for c in commits],
            to_repos=[pfs_proto.Repo(name=r) for r in repos]
            if repos is not None
            else None,
        )

    def subscribe_commit(
        self, repo_name, branch, from_commit_id=None, state=None, prov=None
    ):
        """Yields ``CommitInfo`` objects as commits occur.

        Parameters
        ----------
        repo_name :  str
            The name of the repo.
        branch : str
            The branch to subscribe to.
        from_commit_id : str, optional
            A commit ID. Only commits created since this commit are returned.
        state : int, optional
            The commit state to filter on. See the ``CommitState`` enum.
        prov : CommitProvenance protobuf, optional
            An optional ``CommitProvenance`` object.
        """
        repo = pfs_proto.Repo(name=repo_name)
        req = pfs_proto.SubscribeCommitRequest(
            repo=repo, branch=branch, state=state, prov=prov
        )
        if from_commit_id is not None:
            getattr(req, "from").CopyFrom(
                pfs_proto.Commit(repo=repo, id=from_commit_id)
            )
        return self._req(Service.PFS, "SubscribeCommit", req=req)

    def create_branch(
        self, repo_name, branch_name, commit=None, provenance=None, trigger=None
    ):
        """Creates a new branch.

        Parameters
        ----------
        repo_name : str
            The name of the repo.
        branch_name : str
            The new branch name.
        commit : Union[tuple, str, Commit protobuf], optional
            Represents the head commit of the new branch.
        provenance : List[Branch protobuf], optional
            An optional iterable of `Branch` objects representing the branch
            provenance.
        trigger : Trigger protobuf, optional
            An optional `Trigger` object controlling when the head of
            `branch_name` is moved.
        """
        return self._req(
            Service.PFS,
            "CreateBranch",
            branch=pfs_proto.Branch(
                repo=pfs_proto.Repo(name=repo_name), name=branch_name
            ),
            head=commit_from(commit) if commit is not None else None,
            provenance=provenance,
            trigger=trigger,
        )

    def inspect_branch(self, repo_name, branch_name):
        """Inspects a branch. Returns a ``BranchInfo`` object.

        Parameters
        ----------
        repo_name : str
            The repo name.
        branch_name : str
            The branch name.
        """
        return self._req(
            Service.PFS,
            "InspectBranch",
            branch=pfs_proto.Branch(
                repo=pfs_proto.Repo(name=repo_name), name=branch_name
            ),
        )

    def list_branch(self, repo_name, reverse=None):
        """Lists the active branch objects on a repo. Returns a list of
        ``BranchInfo`` objects.

        Parameters
        ----------
        repo_name : str
            The repo name.
        reverse : bool, optional
            If true, returns branches oldest to newest.
        """
        return self._req(
            Service.PFS,
            "ListBranch",
            repo=pfs_proto.Repo(name=repo_name),
            reverse=reverse,
        ).branch_info

    def delete_branch(self, repo_name, branch_name, force=None):
        """Deletes a branch, but leaves the commits themselves intact. In other
        words, those commits can still be accessed via commit IDs and other
        branches they happen to be on.

        Parameters
        ----------
        repo_name : str
            The repo name.
        branch_name : str
            The name of the branch to delete.
        force : bool, optional
            Whether to force the branch deletion.
        """
        return self._req(
            Service.PFS,
            "DeleteBranch",
            branch=pfs_proto.Branch(
                repo=pfs_proto.Repo(name=repo_name), name=branch_name
            ),
            force=force,
        )

    @contextmanager
    def put_file_client(self):
        """A context manager that gives a :class:`.PutFileClient`. When the
        context manager exits, any operations enqueued from the
        :class:`.PutFileClient` are executed in a single, atomic ``PutFile``
        call.
        """
        pfc = PutFileClient()
        yield pfc
        self._req(Service.PFS, "PutFile", req=pfc._reqs())

    def put_file_bytes(
        self,
        commit,
        path,
        value,
        delimiter=None,
        target_file_datums=None,
        target_file_bytes=None,
        overwrite_index=None,
        header_records=None,
    ):
        """Uploads a PFS file from a file-like object, bytestring, or iterator
        of bytestrings.

        Parameters
        ----------
        commit : Union[tuple, str, Commit protobuf]
            Represents the commit.
        path : str
            The path in the repo the file(s) will be written to.
        value : Union[bytes, BinaryIO]
            The file contents as bytes, represented as a file-like object,
            bytestring, or iterator of bytestrings.
        delimiter : int, optional
            Causes data to be broken up into separate files by the delimiter
            e.g. if you used ``Delimiter.CSV.value``, a separate PFS file will
            be created for each row in the input CSV file, rather than one
            large CSV file.
        target_file_datums : int, optional
            Specifies the target number of datums in each written file. It may
            be lower if data does not split evenly, but will never be higher,
            unless the value is 0.
        target_file_bytes : int, optional
            Specifies the target number of bytes in each written file, file
            may have more or fewer bytes than the target.
        overwrite_index : int, optional
            This is the object index where the write starts from.  All existing
            objects starting from the index are deleted.
        header_records : int, optional
            An optional int for splitting data when `delimiter` is not ``NONE``
            (or ``SQL``). It specifies the number of records that are converted
            to a header and applied to all file shards.
        """
        if isinstance(value, collections.abc.Iterable) and not isinstance(
            value, (str, bytes)
        ):
            warnings.warn(
                "'put_file_bytes' with an iterable 'value' is deprecated, use file-like objects or bytestrings instead",
                DeprecationWarning,
            )
            reqs = put_file_from_iterable_reqs(
                value,
                file=pfs_proto.File(commit=commit_from(commit), path=path),
                delimiter=delimiter,
                target_file_datums=target_file_datums,
                target_file_bytes=target_file_bytes,
                overwrite_index=overwrite_index,
                header_records=header_records,
            )
            return self._req(Service.PFS, "PutFile", req=reqs)

        with self.put_file_client() as pfc:
            if hasattr(value, "read"):
                return pfc.put_file_from_fileobj(
                    commit,
                    path,
                    value,
                    delimiter=delimiter,
                    target_file_datums=target_file_datums,
                    target_file_bytes=target_file_bytes,
                    overwrite_index=overwrite_index,
                    header_records=header_records,
                )
            else:
                return pfc.put_file_from_bytes(
                    commit,
                    path,
                    value,
                    delimiter=delimiter,
                    target_file_datums=target_file_datums,
                    target_file_bytes=target_file_bytes,
                    overwrite_index=overwrite_index,
                    header_records=header_records,
                )

    def put_file_url(
        self,
        commit,
        path,
        url,
        delimiter=None,
        recursive=None,
        target_file_datums=None,
        target_file_bytes=None,
        overwrite_index=None,
        header_records=None,
    ):
        """Puts a file using the content found at a URL. The URL is sent to the
        server which performs the request.

        Parameters
        ----------
        commit : Union[tuple, str, Commit protobuf]
            Represents the commit.
        path : str
            The path in the repo the file will be written to.
        url : str
            The url of the file to put.
        delimiter : int, optional
            Causes data to be broken up into separate files by the delimiter
            e.g. if you used ``Delimiter.CSV.value``, a separate PFS file will
            be created for each row in the input CSV file, rather than one
            large CSV file.
        recursive : bool, optional
            Allow for recursive scraping of some types URLs, for example on
            s3:// URLs.
        target_file_datums : int, optional
            Specifies the target number of datums in each written file. It may
            be lower if data does not split evenly, but will never be higher,
            unless the value is 0.
        target_file_bytes : int, optional
            Specifies the target number of bytes in each written file, file
            may have more or fewer bytes than the target.
        overwrite_index : int, optional
            This is the object index where the write starts from.  All existing
            objects starting from the index are deleted.
        header_records : int, optional
            An optional int for splitting data when `delimiter` is not ``NONE``
            (or ``SQL``). It specifies the number of records that are converted
            to a header and applied to all file shards.
        """

        with self.put_file_client() as pfc:
            pfc.put_file_from_url(
                commit,
                path,
                url,
                delimiter=delimiter,
                recursive=recursive,
                target_file_datums=target_file_datums,
                target_file_bytes=target_file_bytes,
                overwrite_index=overwrite_index,
                header_records=header_records,
            )

    def copy_file(
        self, source_commit, source_path, dest_commit, dest_path, overwrite=None
    ):
        """Efficiently copies files already in PFS. Note that the destination
        repo cannot be an output repo, or the copy operation will (as of 1.9.0)
        silently fail.

        Parameters
        ----------
        source_commit : Union[tuple, str, Commit protobuf]
            Represents the commit with the source file.
        source_path : str
            The path of the source file.
        dest_commit : Union[tuple, str, Commit protobuf]
            Represents the commit for the destination file.
        dest_path : str
            The path of the destination file.
        overwrite : bool, optional
            Whether to overwrite the destination file if it already exists.
        """
        return self._req(
            Service.PFS,
            "CopyFile",
            src=pfs_proto.File(commit=commit_from(source_commit), path=source_path),
            dst=pfs_proto.File(commit=commit_from(dest_commit), path=dest_path),
            overwrite=overwrite,
        )

    def get_file(self, commit, path, offset_bytes=None, size_bytes=None):
        """Returns a :class:`.PFSFile` object, containing the contents of a
        file stored in PFS.

        Parameters
        ----------
        commit : Union[tuple, str, Commit protobuf]
            Represents the commit.
        path : str
            The path of the file.
        offset_bytes : int, optional
            Specifies the number of bytes that should be skipped in the
            beginning of the file.
        size_bytes : int, optional
            Limits the total amount of data returned, note you will get fewer
            bytes than `size_bytes` if you pass a value larger than the size of
            the file. If 0, then all of the data will be returned.
        """
        res = self._req(
            Service.PFS,
            "GetFile",
            file=pfs_proto.File(commit=commit_from(commit), path=path),
            offset_bytes=offset_bytes,
            size_bytes=size_bytes,
        )
        return PFSFile(res)

    def inspect_file(self, commit, path):
        """Inspects a file. Returns a ``FileInfo`` object.

        Parameters
        ----------
        commit : Union[tuple, str, Commit protobuf]
            Represents the commit.
        path : str
            The path to the file.
        """
        return self._req(
            Service.PFS,
            "InspectFile",
            file=pfs_proto.File(commit=commit_from(commit), path=path),
        )

    def list_file(self, commit, path, history=None, include_contents=None):
        """.. # noqa: W505

        Lists the files in a directory.

        Parameters
        ----------
        commit : Union[tuple, str, Commit protobuf]
            Represents the commit.
        path : str
            The path to the directory.
        history : int, optional
            Indicates how many historical versions you want returned.
            Semantics are:

            - 0: Return the files as they are in `commit`
            - 1: Return above and the files as they are in the last commit they were modified in.
            - 2: etc.
            - -1: Return all historical versions.

        include_contents : bool, optional
            If `True`, file contents are included.
        """
        return self._req(
            Service.PFS,
            "ListFileStream",
            file=pfs_proto.File(commit=commit_from(commit), path=path),
            history=history,
            full=include_contents,
        )

    def walk_file(self, commit, path):
        """Walks over all descendant files in a directory. Returns a generator
        of ``FileInfo`` objects.

        Parameters
        ----------
        commit : Union[tuple, str, Commit protobuf]
            Represents the commit.
        path : str
            The path to the directory.
        """
        return self._req(
            Service.PFS,
            "WalkFile",
            file=pfs_proto.File(commit=commit_from(commit), path=path),
        )

    def glob_file(self, commit, pattern):
        """Lists files that match a glob pattern. Yields ``FileInfo`` objects.

        Parameters
        ----------
        commit : Union[tuple, str, Commit protobuf]
            Represents the commit.
        pattern : str
            The glob pattern.
        """
        return self._req(
            Service.PFS, "GlobFileStream", commit=commit_from(commit), pattern=pattern
        )

    def delete_file(self, commit, path):
        """Deletes a file from a Commit. DeleteFile leaves a tombstone in the
        Commit, assuming the file isn't written to later attempting to get the
        file from the finished commit will result in not found error. The file
        will of course remain intact in the Commit's parent.

        Parameters
        ----------
        commit : Union[tuple, str, Commit protobuf]
            Represents the commit.
        path : str
            The path to the file.
        """
        return self._req(
            Service.PFS,
            "DeleteFile",
            file=pfs_proto.File(commit=commit_from(commit), path=path),
        )

    def fsck(self, fix=None):
        """Performs a file system consistency check for PFS."""
        return self._req(Service.PFS, "Fsck", fix=fix)

    def diff_file(
        self, new_commit, new_path, old_commit=None, old_path=None, shallow=None
    ):
        """Diffs two files. If `old_commit` or `old_path` are not specified,
        the same path in the parent of the file specified by `new_commit` and
        `new_path` will be used.

        Parameters
        ----------
        new_commit : Union[tuple, str, Commit protobuf]
            Represents the commit for the new file.
        new_path : str
            The path of the new file.
        old_commit : Union[tuple, str, Commit protobuf]
            Represents the commit for the old file.
        old_path : str
            The path of the old file.
        shallow : bool, optional
            Whether to do a shallow diff.
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

    def create_tmp_file_set(self):
        """Creates a temporary fileset (used internally). Currently,
        temp-fileset-related APIs are only used for Pachyderm internals (job
        merging), so we're avoiding support for these functions until we find a
        use for them (feel free to file an issue in
        github.com/pachyderm/pachyderm)
        """
        raise NotImplementedError("temporary filesets are internal-use-only")

    def renew_tmp_file_set(self, fileset_id, ttl_seconds):
        """Renews a temporary fileset (used internally). Currently,
        temp-fileset-related APIs are only used for Pachyderm internals (job
        merging), so we're avoiding support for these functions until we find a
        use for them (feel free to file an issue in
        github.com/pachyderm/pachyderm)

        Parameters
        ----------
        fileset_id : str
            The fileset ID.
        ttl_seconds : int
            The number of seconds to keep alive the temporary fileset.
        """
        raise NotImplementedError("temporary filesets are internal-use-only")


class PutFileClient:
    """
    :class:`.PutFileClient` puts or deletes PFS files atomically.
    """

    def __init__(self):
        self._ops = []

    def _reqs(self):
        for op in self._ops:
            yield from op.reqs()

    def put_file_from_filepath(
        self,
        commit,
        pfs_path,
        local_path,
        delimiter=None,
        target_file_datums=None,
        target_file_bytes=None,
        overwrite_index=None,
        header_records=None,
    ):
        """Uploads a PFS file from a local path at a specified path. This will
        lazily open files, which will prevent too many files from being
        opened, or too much memory being consumed, when atomically putting
        many files.

        Parameters
        ----------
        commit : Union[tuple, str, Commit protobuf]
            Represents the commit.
        pfs_path : str
            The path in the repo to upload the file to will be written to.
        local_path : str
            The local file path.
        delimiter : int, optional
            Causes data to be broken up into separate files by the delimiter
            e.g. if you used ``Delimiter.CSV.value``, a separate PFS file will
            be created for each row in the input CSV file, rather than one
            large CSV file.
        target_file_datums : int, optional
            Specifies the target number of datums in each written file. It may
            be lower if data does not split evenly, but will never be higher,
            unless the value is 0.
        target_file_bytes : int, optional
            Specifies the target number of bytes in each written file, file
            may have more or fewer bytes than the target.
        overwrite_index : int, optional
            This is the object index where the write starts from.  All existing
            objects starting from the index are deleted.
        header_records : int, optional
            An optional int for splitting data when `delimiter` is not ``NONE``
            (or ``SQL``). It specifies the number of records that are converted
            to a header and applied to all file shards.
        """
        self._ops.append(
            AtomicPutFilepathOp(
                commit,
                pfs_path,
                local_path,
                delimiter=delimiter,
                target_file_datums=target_file_datums,
                target_file_bytes=target_file_bytes,
                overwrite_index=pfs_proto.OverwriteIndex(index=overwrite_index)
                if overwrite_index is not None
                else None,
                header_records=header_records,
            )
        )

    def put_file_from_fileobj(
        self,
        commit,
        path,
        value,
        delimiter=None,
        target_file_datums=None,
        target_file_bytes=None,
        overwrite_index=None,
        header_records=None,
    ):
        """Uploads a PFS file from a file-like object.

        Parameters
        ----------
        commit : Union[tuple, str, Commit protobuf]
            Represents the commit.
        path : str
            The path in the repo to upload the file to will be written to.
        value : BinaryIO
            The file-like object.
        delimiter : int, optional
            Causes data to be broken up into separate files by the delimiter
            e.g. if you used ``Delimiter.CSV.value``, a separate PFS file will
            be created for each row in the input CSV file, rather than one
            large CSV file.
        target_file_datums : int, optional
            Specifies the target number of datums in each written file. It may
            be lower if data does not split evenly, but will never be higher,
            unless the value is 0.
        target_file_bytes : int, optional
            Specifies the target number of bytes in each written file, file
            may have more or fewer bytes than the target.
        overwrite_index : int, optional
            This is the object index where the write starts from.  All existing
            objects starting from the index are deleted.
        header_records : int, optional
            An optional int for splitting data when `delimiter` is not ``NONE``
            (or ``SQL``). It specifies the number of records that are converted
            to a header and applied to all file shards.
        """
        self._ops.append(
            AtomicPutFileobjOp(
                commit,
                path,
                value,
                delimiter=delimiter,
                target_file_datums=target_file_datums,
                target_file_bytes=target_file_bytes,
                overwrite_index=pfs_proto.OverwriteIndex(index=overwrite_index)
                if overwrite_index is not None
                else None,
                header_records=header_records,
            )
        )

    def put_file_from_bytes(
        self,
        commit,
        path,
        value,
        delimiter=None,
        target_file_datums=None,
        target_file_bytes=None,
        overwrite_index=None,
        header_records=None,
    ):
        """Uploads a PFS file from a bytestring.

        Parameters
        ----------
        commit : Union[tuple, str, Commit protobuf]
            Represents the commit.
        path : str
            The path in the repo to upload the file to will be written to.
        value : bytes
            The file contents as a bytestring.
        delimiter : int, optional
            Causes data to be broken up into separate files by the delimiter
            e.g. if you used ``Delimiter.CSV.value``, a separate PFS file will
            be created for each row in the input CSV file, rather than one
            large CSV file.
        target_file_datums : int, optional
            Specifies the target number of datums in each written file. It may
            be lower if data does not split evenly, but will never be higher,
            unless the value is 0.
        target_file_bytes : int, optional
            Specifies the target number of bytes in each written file, file
            may have more or fewer bytes than the target.
        overwrite_index : int, optional
            This is the object index where the write starts from.  All existing
            objects starting from the index are deleted.
        header_records : int, optional
            An optional int for splitting data when `delimiter` is not ``NONE``
            (or ``SQL``). It specifies the number of records that are converted
            to a header and applied to all file shards.
        """
        self.put_file_from_fileobj(
            commit,
            path,
            io.BytesIO(value),
            delimiter=delimiter,
            target_file_datums=target_file_datums,
            target_file_bytes=target_file_bytes,
            overwrite_index=overwrite_index,
            header_records=header_records,
        )

    def put_file_from_url(
        self,
        commit,
        path,
        url,
        delimiter=None,
        recursive=None,
        target_file_datums=None,
        target_file_bytes=None,
        overwrite_index=None,
        header_records=None,
    ):
        """Puts a file using the content found at a URL. The URL is sent to the
        server which performs the request.

        Parameters
        ----------
        commit : Union[tuple, str, Commit protobuf]
            Represents the commit.
        path : str
            The path in the repo the file will be written to.
        url : str
            The url of the file to put.
        delimiter : int, optional
            Causes data to be broken up into separate files by the delimiter
            e.g. if you used ``Delimiter.CSV.value``, a separate PFS file will
            be created for each row in the input CSV file, rather than one
            large CSV file.
        recursive : bool, optional
            Allow for recursive scraping of some types URLs, for example on
            s3:// URLs.
        target_file_datums : int, optional
            Specifies the target number of datums in each written file. It may
            be lower if data does not split evenly, but will never be higher,
            unless the value is 0.
        target_file_bytes : int, optional
            Specifies the target number of bytes in each written file, file
            may have more or fewer bytes than the target.
        overwrite_index : int, optional
            This is the object index where the write starts from.  All existing
            objects starting from the index are deleted.
        header_records : int, optional
            An optional int for splitting data when `delimiter` is not ``NONE``
            (or ``SQL``). It specifies the number of records that are converted
            to a header and applied to all file shards.
        """
        self._ops.append(
            AtomicOp(
                commit,
                path,
                url=url,
                delimiter=delimiter,
                recursive=recursive,
                target_file_datums=target_file_datums,
                target_file_bytes=target_file_bytes,
                overwrite_index=pfs_proto.OverwriteIndex(index=overwrite_index)
                if overwrite_index is not None
                else None,
                header_records=header_records,
            )
        )

    def delete_file(self, commit, path):
        """Deletes a file.

        Parameters
        ----------
        commit : Union[tuple, str, Commit protobuf]
            Represents the commit.
        path : str
            The path to the file.
        """
        self._ops.append(AtomicOp(commit, path, delete=True))


class AtomicOp:
    """
    Represents an operation in a ``PutFile`` call.
    """

    def __init__(self, commit, path, **kwargs):
        kwargs["file"] = pfs_proto.File(commit=commit_from(commit), path=path)
        self.kwargs = kwargs

    def reqs(self):
        """
        Yields one or more protobuf ``PutFileRequests``, which are then enqueued
        into the request's channel.
        """
        yield pfs_proto.PutFileRequest(**self.kwargs)


class AtomicPutFilepathOp(AtomicOp):
    """
    A ``PutFile`` operation to put a file locally stored at a given path. This
    file is opened on-demand, which helps with minimizing the number of open
    files.
    """

    def __init__(self, commit, pfs_path, local_path, **kwargs):
        super().__init__(commit, pfs_path, **kwargs)
        self.local_path = local_path

    def reqs(self):
        with open(self.local_path, "rb") as f:
            yield from put_file_from_fileobj_reqs(f, **self.kwargs)


class AtomicPutFileobjOp(AtomicOp):
    """A ``PutFile`` operation to put a file from a file-like object."""

    def __init__(self, commit, path, value, **kwargs):
        super().__init__(commit, path, **kwargs)
        self.value = value

    def reqs(self):
        yield from put_file_from_fileobj_reqs(self.value, **self.kwargs)


def put_file_from_fileobj_reqs(fileish, **kwargs):
    chunked_iter = itertools.takewhile(
        lambda chunk: len(chunk) > 0, map(fileish.read, itertools.repeat(BUFFER_SIZE))
    )
    return put_file_from_iterable_reqs(chunked_iter, **kwargs)


def put_file_from_iterable_reqs(value, **kwargs):
    for i, chunk in enumerate(itertools.chain(value, [None])):
        if i == 0:
            yield pfs_proto.PutFileRequest(value=chunk, **kwargs)
        elif chunk is not None:
            yield pfs_proto.PutFileRequest(value=chunk)
