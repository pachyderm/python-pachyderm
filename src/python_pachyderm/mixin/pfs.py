import io
import itertools
import tarfile
from contextlib import contextmanager

from python_pachyderm.proto.pfs import pfs_pb2 as pfs_proto
from python_pachyderm.service import Service
from .util import commit_from


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

    def __init__(self, stream):
        f = tarfile.open(fileobj=stream, mode='r|*')
        self._file = f.extractfile(f.next())

    def read(self, size=-1):
        return self._file.read(size)


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
        return self._req(
            Service.PFS, "CreateRepo",
            repo=pfs_proto.Repo(name=repo_name),
            description=description,
            update=update,
        )

    def inspect_repo(self, repo_name):
        """
        Returns info about a specific repo. Returns a `RepoInfo` object.

        Params:

        * `repo_name`: Name of the repo.
        """
        return self._req(Service.PFS, "InspectRepo", repo=pfs_proto.Repo(name=repo_name))

    def list_repo(self):
        """
        Returns info about all repos, as a list of `RepoInfo` objects.
        """
        return self._req(Service.PFS, "ListRepo").repo_info

    def delete_repo(self, repo_name, force=None):
        """
        Deletes a repo and reclaims the storage space it was using.

        Params:

        * `repo_name`: The name of the repo.
        * `force`: If set to true, the repo will be removed regardless of
          errors. This argument should be used with care.
        """
        return self._req(Service.PFS, "DeleteRepo",
                         repo=pfs_proto.Repo(name=repo_name), force=force,
                         all=False)

    def delete_all_repos(self, force=None):
        """
        Deletes all repos.

        Params:

        * `force`: If set to true, the repo will be removed regardless of
        errors. This argument should be used with care.
        """
        return self._req(Service.PFS, "DeleteRepo", force=force, all=True)

    def start_commit(self, repo_name, branch=None, parent=None, description=None, provenance=None):
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
        * `provenance`: An optional iterable of `CommitProvenance` objects
        specifying the commit provenance.
        """
        return self._req(
            Service.PFS, "StartCommit",
            parent=pfs_proto.Commit(repo=pfs_proto.Repo(name=repo_name), id=parent),
            branch=branch,
            description=description,
            provenance=provenance,
        )

    def finish_commit(self, commit, description=None, size_bytes=None, empty=None):
        """
        Ends the process of committing data to a Repo and persists the
        Commit. Once a Commit is finished the data becomes immutable and
        future attempts to write to it with ModifyFile will error.

        Params:

        * `commit`: A tuple, string, or `Commit` object representing the
        commit.
        * `description`: An optional string describing this commit.
        * `size_bytes`: An optional int.
        * `empty`: An optional bool. If set, the commit will be closed (its
        `finished` field will be set to the current time) but its `tree` will
        be left nil.
        """
        return self._req(
            Service.PFS, "FinishCommit",
            commit=commit_from(commit),
            description=description,
            size_bytes=size_bytes,
            empty=empty,
        )

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
        * An optional int that causes this method to block until the commit is
        in the desired commit state. See the `CommitState` enum.
        """
        return self._req(Service.PFS, "InspectCommit", commit=commit_from(commit), block_state=block_state)

    def list_commit(self, repo_name, to_commit=None, from_commit=None, number=None, reverse=None):
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
        req = pfs_proto.ListCommitRequest(repo=pfs_proto.Repo(name=repo_name), number=number, reverse=reverse)
        if to_commit is not None:
            req.to.CopyFrom(commit_from(to_commit))
        if from_commit is not None:
            getattr(req, 'from').CopyFrom(commit_from(from_commit))
        return self._req(Service.PFS, "ListCommit", req=req)

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
        return self._req(
            Service.PFS, "FlushCommit",
            commits=[commit_from(c) for c in commits],
            to_repos=[pfs_proto.Repo(name=r) for r in repos] if repos is not None else None,
        )

    def subscribe_commit(self, repo_name, branch, from_commit_id=None, state=None, prov=None):
        """
        Yields `CommitInfo` objects as commits occur.

        Params:

        * `repo_name`: A string specifying the name of the repo.
        * `branch`: A string specifying branch to subscribe to.
        * `from_commit_id`: An optional string specifying the commit ID. Only
        commits created since this commit are returned.
        * `state`: The commit state to filter on.
        * `prov`: An optional `CommitProvenance` object.
        """
        repo = pfs_proto.Repo(name=repo_name)
        req = pfs_proto.SubscribeCommitRequest(repo=repo, branch=branch, state=state, prov=prov)
        if from_commit_id is not None:
            getattr(req, 'from').CopyFrom(pfs_proto.Commit(repo=repo, id=from_commit_id))
        return self._req(Service.PFS, "SubscribeCommit", req=req)

    def create_branch(self, repo_name, branch_name, commit=None,
                      provenance=None, trigger=None):
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
        """
        return self._req(
            Service.PFS, "CreateBranch",
            branch=pfs_proto.Branch(repo=pfs_proto.Repo(name=repo_name), name=branch_name),
            head=commit_from(commit) if commit is not None else None,
            provenance=provenance, trigger=trigger,
        )

    def inspect_branch(self, repo_name, branch_name):
        """
        Inspects a branch. Returns a `BranchInfo` object.
        """
        return self._req(
            Service.PFS, "InspectBranch",
            branch=pfs_proto.Branch(repo=pfs_proto.Repo(name=repo_name), name=branch_name),
        )

    def list_branch(self, repo_name, reverse=None):
        """
        Lists the active branch objects on a repo. Returns a list of
        `BranchInfo` objects.

        Params:

        * `repo_name`: A string specifying the repo name.
        """
        return self._req(Service.PFS, "ListBranch", repo=pfs_proto.Repo(name=repo_name), reverse=reverse).branch_info

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
        return self._req(
            Service.PFS, "DeleteBranch",
            branch=pfs_proto.Branch(repo=pfs_proto.Repo(name=repo_name), name=branch_name),
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

    def put_file_bytes(self, commit, path, value, delimiter=None, target_file_datums=None,
                       target_file_bytes=None, append=None, header_records=None):
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
        * `delimiter`: An optional int. causes data to be broken up into
        separate files by the delimiter. e.g. if you used
        `Delimiter.CSV.value`, a separate PFS file will be created for each
        row in the input CSV file, rather than one large CSV file.
        * `target_file_datums`: An optional int. Specifies the target number of
        datums in each written file. It may be lower if data does not split
        evenly, but will never be higher, unless the value is 0.
        * `target_file_bytes`: An optional int. Specifies the target number of
        bytes in each written file, files may have more or fewer bytes than
        the target.
        * `append`: An optional bool, if true the data is appended to the file,
        if it already exists.
        * `header_records: An optional int for splitting data when `delimiter`
        is not `NONE` (or `SQL`). It specifies the number of records that are
        converted to a header and applied to all file shards.
        """
        with self.modify_file_client(commit) as pfc:
            if hasattr(value, "read"):
                return pfc.put_file_from_fileobj(
                    path, value,
                    # delimiter=delimiter,
                    # target_file_datums=target_file_datums,
                    # target_file_bytes=target_file_bytes,
                    # header_records=header_records,
                )
            else:
                return pfc.put_file_from_bytes(
                    path, value,
                    # delimiter=delimiter,
                    # target_file_datums=target_file_datums,
                    # target_file_bytes=target_file_bytes,
                    # header_records=header_records,
                )

    def put_file_url(self, commit, path, url, delimiter=None, recursive=None, target_file_datums=None,
                     target_file_bytes=None, append=None, header_records=None):
        """
        Puts a file using the content found at a URL. The URL is sent to the
        server which performs the request.

        Params:

        * `commit`: A tuple, string, or `Commit` object representing the
        commit.
        * `path`: A string specifying the path to the file.
        * `url`: A string specifying the url of the file to put.
        * `delimiter`: An optional int. causes data to be broken up into
        separate files by the delimiter. e.g. if you used
        `Delimiter.CSV.value`, a separate PFS file will be created for each
        row in the input CSV file, rather than one large CSV file.
        * `recursive`: allow for recursive scraping of some types URLs, for
        example on s3:// URLs.
        * `target_file_datums`: An optional int. Specifies the target number of
        datums in each written file. It may be lower if data does not split
        evenly, but will never be higher, unless the value is 0.
        * `target_file_bytes`: An optional int. Specifies the target number of
        bytes in each written file, files may have more or fewer bytes than
        the target.
        * `append`: An optional bool, if true the data is appended to the file,
        if it already exists.
        * `header_records: An optional int for splitting data when `delimiter`
        is not `NONE` (or `SQL`). It specifies the number of records that are
        converted to a header and applied to all file shards.
        """

        with self.modify_file_client(commit) as pfc:
            pfc.put_file_from_url(
                path, url,
                recursive=recursive,
                append=append,
                # delimiter=delimiter,
                # target_file_datums=target_file_datums,
                # target_file_bytes=target_file_bytes,
                # header_records=header_records,
            )

    def copy_file(self, source_commit, source_path, dest_commit, dest_path, append=None, tag=None):
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
        * `append`: An optional bool, if true the data is appended to the file,
        if it already exists.
        """
        with self.modify_file_client(dest_commit) as pfc:
            pfc.copy_file(source_commit, source_path, dest_path, append=append, tag=tag)

    def get_file(self, commit, path, URL=None):
        """
        Returns a `PFSFile` object, containing the contents of a file stored
        in PFS.

        Params:

        * `commit`: A tuple, string, or `Commit` object representing the
        commit.
        * `path`: A string specifying the path of the file.
        """
        res = self._req(
            Service.PFS, "GetFile",
            file=pfs_proto.File(commit=commit_from(commit), path=path, url=URL),
        )
        return PFSFile(FileTarstream(res))

    def inspect_file(self, commit, path):
        """
        Inspects a file. Returns a `FileInfo` object.

        Params:

        * `commit`: A tuple, string, or `Commit` object representing the
        commit.
        * `path`: A string specifying the path to the file.
        """
        return self._req(Service.PFS, "InspectFile", file=pfs_proto.File(commit=commit_from(commit), path=path))

    def list_file(self, commit, path, include_contents=None):
        """
        Lists the files in a directory.

        Params:

        * `commit`: A tuple, string, or `Commit` object representing the
        commit.
        * `path`: The path to the directory.
        * `include_contents`: An optional bool. If `True`, file contents are
        included.
        """
        return self._req(
            Service.PFS, "ListFile",
            file=pfs_proto.File(commit=commit_from(commit), path=path),
            full=include_contents,
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
        return self._req(Service.PFS, "WalkFile", file=pfs_proto.File(commit=commit_from(commit), path=path))

    def glob_file(self, commit, pattern):
        """
        Lists files that match a glob pattern. Yields `FileInfo` objects.

        Params:

        * `commit`: A tuple, string, or `Commit` object representing the
        commit.
        * `pattern`: A string representing a glob pattern.
        """
        return self._req(Service.PFS, "GlobFile", commit=commit_from(commit), pattern=pattern)

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
            return pfc.delete_file(path)

    def fsck(self, fix=None):
        """
        Performs a file system consistency check for PFS.
        """
        return self._req(Service.PFS, "Fsck", fix=fix)

    def diff_file(self, new_commit, new_path, old_commit=None, old_path=None, shallow=None):
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
            Service.PFS, "DiffFile",
            new_file=pfs_proto.File(commit=commit_from(new_commit), path=new_path),
            old_file=old_file,
            shallow=shallow,
        )

    def create_tmp_file_set(self):
        """
        Creates a temporary fileset (used internally). Currently,
        temp-fileset-related APIs are only used for Pachyderm internals (job
        merging), so we're avoiding support for these functions until we find a
        use for them (feel free to file an issue in
        github.com/pachyderm/pachyderm)

        Params:

        * `fileset_id`: A string identifying the fileset.
        """
        raise NotImplementedError('temporary filesets are internal-use-only')

    def renew_tmp_file_set(self, fileset_id, ttl_seconds):
        """
        Renews a temporary fileset (used internally). Currently,
        temp-fileset-related APIs are only used for Pachyderm internals (job
        merging), so we're avoiding support for these functions until we find a
        use for them (feel free to file an issue in
        github.com/pachyderm/pachyderm)

        Params:

        * `fileset_id`: A string identifying the fileset.
        * `ttl_seconds`: A int determining the number of seconds to keep alive
        the temporary fileset
        """
        raise NotImplementedError('temporary filesets are internal-use-only')


class ModifyFileClient:
    """
    `ModifyFileClient` puts or deletes PFS files atomically.
    """

    def __init__(self, commit):
        self._ops = []
        self.commit = commit_from(commit)

    def _reqs(self):
        for op in self._ops:
            for r in op.reqs():
                yield r

    def put_file_from_filepath(self, pfs_path, local_path, append=None, delimiter=None, target_file_datums=None,
                               target_file_bytes=None, header_records=None):
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
        * `delimiter`: An optional int. causes data to be broken up into
        separate files by the delimiter. e.g. if you used
        `Delimiter.CSV.value`, a separate PFS file will be created for each
        row in the input CSV file, rather than one large CSV file.
        * `target_file_datums`: An optional int. Specifies the target number of
        datums in each written file. It may be lower if data does not split
        evenly, but will never be higher, unless the value is 0.
        * `target_file_bytes`: An optional int. Specifies the target number of
        bytes in each written file, files may have more or fewer bytes than
        the target.
        * `header_records: An optional int for splitting data when `delimiter`
        is not `NONE` (or `SQL`). It specifies the number of records that are
        converted to a header and applied to all file shards.
        """
        self._ops.append(AtomicModifyFilepathOp(
            self.commit, pfs_path, local_path, append,
            # delimiter=delimiter,
            # target_file_datums=target_file_datums,
            # target_file_bytes=target_file_bytes,
            # header_records=header_records,
        ))

    def put_file_from_fileobj(self, path, value, append=None, delimiter=None, target_file_datums=None,
                              target_file_bytes=None, header_records=None):
        """
        Uploads a PFS file from a file-like object.

        Params:

        * `path`: A string specifying the path in the repo the file(s) will be
        written to.
        * `value`: The file-like object.
        * `append`: An optional bool, if true the data is appended to the file,
        if it already exists.
        * `delimiter`: An optional int. causes data to be broken up into
        separate files by the delimiter. e.g. if you used
        `Delimiter.CSV.value`, a separate PFS file will be created for each
        row in the input CSV file, rather than one large CSV file.
        * `target_file_datums`: An optional int. Specifies the target number of
        datums in each written file. It may be lower if data does not split
        evenly, but will never be higher, unless the value is 0.
        * `target_file_bytes`: An optional int. Specifies the target number of
        bytes in each written file, files may have more or fewer bytes than
        the target.
        * `header_records: An optional int for splitting data when `delimiter`
        is not `NONE` (or `SQL`). It specifies the number of records that are
        converted to a header and applied to all file shards.
        """
        self._ops.append(AtomicModifyFileobjOp(
            self.commit, path, value, append,
            # delimiter=delimiter,
            # target_file_datums=target_file_datums,
            # target_file_bytes=target_file_bytes,
            # header_records=header_records,
        ))

    def put_file_from_bytes(self, path, value, append=None, delimiter=None, target_file_datums=None,
                            target_file_bytes=None, header_records=None):
        """
        Uploads a PFS file from a bytestring.

        Params:

        * `path`: A string specifying the path in the repo the file(s) will be
        written to.
        * `value`: The file contents as a bytestring.
        * `append`: An optional bool, if true the data is appended to the file,
        if it already exists.
        * `delimiter`: An optional int. causes data to be broken up into
        separate files by the delimiter. e.g. if you used
        `Delimiter.CSV.value`, a separate PFS file will be created for each
        row in the input CSV file, rather than one large CSV file.
        * `target_file_datums`: An optional int. Specifies the target number of
        datums in each written file. It may be lower if data does not split
        evenly, but will never be higher, unless the value is 0.
        * `target_file_bytes`: An optional int. Specifies the target number of
        bytes in each written file, files may have more or fewer bytes than
        the target.
        * `header_records: An optional int for splitting data when `delimiter`
        is not `NONE` (or `SQL`). It specifies the number of records that are
        converted to a header and applied to all file shards.
        """
        self.put_file_from_fileobj(
            path, io.BytesIO(value), append,
            # delimiter=delimiter,
            # target_file_datums=target_file_datums,
            # target_file_bytes=target_file_bytes,
            # header_records=header_records,
        )

    def put_file_from_url(self, path, url, append=None, delimiter=None, recursive=None, target_file_datums=None,
                          target_file_bytes=None, header_records=None):
        """
        Puts a file using the content found at a URL. The URL is sent to the
        server which performs the request.

        Params:

        * `path`: A string specifying the path to the file.
        * `url`: A string specifying the url of the file to put.
        * `append`: An optional bool, if true the data is appended to the file,
        if it already exists.
        * `delimiter`: An optional int. causes data to be broken up into
        separate files by the delimiter. e.g. if you used
        `Delimiter.CSV.value`, a separate PFS file will be created for each
        row in the input CSV file, rather than one large CSV file.
        * `recursive`: allow for recursive scraping of some types URLs, for
        example on s3:// URLs.
        * `target_file_datums`: An optional int. Specifies the target number of
        datums in each written file. It may be lower if data does not split
        evenly, but will never be higher, unless the value is 0.
        * `target_file_bytes`: An optional int. Specifies the target number of
        bytes in each written file, files may have more or fewer bytes than
        the target.
        * `header_records: An optional int for splitting data when `delimiter`
        is not `NONE` (or `SQL`). It specifies the number of records that are
        converted to a header and applied to all file shards.
        """
        self._ops.append(AtomicModifyFileURLOp(
            self.commit, path, url, append,
            recursive=recursive,
            # delimiter=delimiter,
            # target_file_datums=target_file_datums,
            # target_file_bytes=target_file_bytes,
            # header_records=header_records,
        ))

    def delete_file(self, path):
        """
        Deletes a file.

        Params:

        * `path`: The path to the file.
        """
        self._ops.append(AtomicDeleteFileOp(self.commit, path))

    def copy_file(self, source_commit, source_path, dest_path, append=None, tag=None):
        """
        Copy a file.

        Params:

        * `source_commit`: The commit the source file is in.
        * `source_path`: The path to the source file.
        * `dest_path`: The path to the destination file.
        * `append`: An optional bool, if true the data is appended to the file,
        if it already exists.
        """
        self._ops.append(AtomicCopyFileOp(self.commit, source_commit, source_path, dest_path, append=append, tag=tag))


class AtomicOp:
    """
    Represents an operation in a `ModifyFile` call.
    """

    def __init__(self, commit, path):
        self.commit = commit
        self.path = path

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

    def __init__(self, commit, pfs_path, local_path, append):
        super().__init__(commit, pfs_path)
        self.local_path = local_path
        self.append = append

    def reqs(self):
        with open(self.local_path, "rb") as f:
            for i, chunk in enumerate(f):
                yield put_file_req(commit=self.commit, path=self.path, chunk=chunk)
        yield put_file_req(commit=self.commit, path=self.path, eof=True)


class AtomicModifyFileobjOp(AtomicOp):
    """A `ModifyFile` operation to put a file from a file-like object."""

    def __init__(self, commit, path, value, append, **kwargs):
        super().__init__(commit, path, **kwargs)
        self.value = value
        self.append = append

    def reqs(self):
        for i in itertools.count():
            chunk = self.value.read(BUFFER_SIZE)
            if len(chunk) == 0:
                yield put_file_req(commit=self.commit, path=self.path, eof=True)
                return
            yield put_file_req(commit=self.commit, path=self.path, chunk=chunk)


class AtomicModifyFileURLOp(AtomicOp):
    """A `ModifyFile` operation to put a file from a URL."""
    def __init__(self, commit, path, url, append, recursive=False, **kwargs):
        super().__init__(commit, path, **kwargs)
        self.url = url
        self.recursive = recursive
        self.append = append

    def reqs(self):
        yield pfs_proto.ModifyFileRequest(
          commit=self.commit,
          put_file=pfs_proto.PutFile(
             url_file_source=pfs_proto.URLFileSource(path=self.path, URL=self.url, recursive=self.recursive),
             append=self.append,
          )
        )


class AtomicCopyFileOp(AtomicOp):
    """A `ModifyFile` operation to copy a file."""
    def __init__(self, target_commit, source_commit, source_path, dest_path, append, tag):
        super().__init__(target_commit, dest_path)
        self.source_commit = commit_from(source_commit)
        self.source_path = source_path
        self.dest_path = dest_path
        self.tag = tag
        self.append = append

    def reqs(self):
        yield pfs_proto.ModifyFileRequest(
          commit=self.commit, 
          copy_file=pfs_proto.CopyFile(
            append=self.append,
            tag=self.tag,
            dst=self.dest_path,
            src=pfs_proto.File(commit=self.source_commit, path=self.source_path),
          )
        )


class AtomicDeleteFileOp(AtomicOp):
    """A `ModifyFile` operation to delete a file."""
    def __init__(self, commit, pfs_path):
        super().__init__(commit, pfs_path)

    def reqs(self):
        yield pfs_proto.ModifyFileRequest(commit=self.commit, delete_file=pfs_proto.DeleteFile(file=self.path))


def put_file_req(commit=None, path=None, chunk=None, append=False, eof=False):
    return pfs_proto.ModifyFileRequest(
        commit=commit,
        put_file=pfs_proto.PutFile(append=append, raw_file_source=pfs_proto.RawFileSource(path=path, data=chunk, EOF=eof))
    )
