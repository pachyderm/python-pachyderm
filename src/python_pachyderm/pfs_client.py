# -*- coding: utf-8 -*-

from __future__ import absolute_import

import collections
import itertools
from contextlib import contextmanager

import six

from python_pachyderm.client.pfs import pfs_pb2 as proto
from python_pachyderm.client.pfs import pfs_pb2_grpc as grpc
from python_pachyderm.util import commit_from, get_address, get_metadata

BUFFER_SIZE = 3 * 1024 * 1024  # 3MB TODO: Base this on some grpc value


class ExtractValueIterator(object):
    def __init__(self, r):
        self._iter = r

    def __iter__(self):
        for item in self._iter:
            yield item.value


class PfsClient(object):
    def __init__(self, host=None, port=None, auth_token=None):
        """
        Creates a client to connect to PFS.
        
        Params:
        * host: The pachd host. Default is 'localhost', which is used with
        `pachctl port-forward`.
        * port: The port to connect to. Default is 30650.
        * auth_token: The authentication token; used if authentication is
        enabled on the cluster. Default to `None`.
        """

        address = get_address(host, port)
        self.metadata = get_metadata(auth_token)
        self.channel = grpc.grpc.insecure_channel(address)
        self.stub = grpc.APIStub(self.channel)

    def create_repo(self, repo_name, description=None):
        """
        Creates a new Repo object in PFS with the given name. Repos are the 
        top level data object in PFS and should be used to store data of a
        similar type. For example rather than having a single Repo for an
        entire project you might have separate Repos for logs, metrics,
        database dumps etc.
        
        Params:
        * repo_name: Name of the repo.
        * description: Repo description.
        """
        req = proto.CreateRepoRequest(repo=proto.Repo(name=repo_name), description=description)
        self.stub.CreateRepo(req, metadata=self.metadata)

    def inspect_repo(self, repo_name):
        """
        Returns info about a specific Repo.
        
        Params:
        * repo_name: Name of the repo.
        """
        req = proto.InspectRepoRequest(repo=proto.Repo(name=repo_name))
        res = self.stub.InspectRepo(req, metadata=self.metadata)
        return res

    def list_repo(self):
        """
        Returns info about all Repos.
        """
        req = proto.ListRepoRequest()
        res = self.stub.ListRepo(req, metadata=self.metadata)
        if hasattr(res, 'repo_info'):
            return res.repo_info
        return []

    def delete_repo(self, repo_name=None, force=False, all=False):
        """
        Deletes a repo and reclaims the storage space it was using.

        Params:
        * repo_name: The name of the repo.
        * force: If set to true, the repo will be removed regardless of
        errors. This argument should be used with care.
        * all: Delete all repos.
        """
        if not all:
            if repo_name:
                req = proto.DeleteRepoRequest(repo=proto.Repo(name=repo_name), force=force)
                self.stub.DeleteRepo(req, metadata=self.metadata)
            else:
                raise ValueError("Either a repo_name or all=True needs to be provided")
        else:
            if not repo_name:
                req = proto.DeleteRepoRequest(force=force, all=all)
                self.stub.DeleteRepo(req, metadata=self.metadata)
            else:
                raise ValueError("Cannot specify a repo_name if all=True")

    def start_commit(self, repo_name, branch=None, parent=None, description=None):
        """
        Begins the process of committing data to a Repo. Once started you can
        write to the Commit with PutFile and when all the data has been
        written you must finish the Commit with FinishCommit. NOTE, data is
        not persisted until FinishCommit is called. A Commit object is
        returned.

        Params:
        * repo_name: The name of the repo.
        * branch: A more convenient way to build linear chains of commits.
        When a commit is started with a non-empty branch the value of branch
        becomes an alias for the created Commit. This enables a more intuitive
        access pattern. When the commit is started on a branch the previous
        head of the branch is used as the parent of the commit.
        * parent: Specifies the parent Commit, upon creation the new Commit
        will appear identical to the parent Commit, data can safely be added
        to the new commit without affecting the contents of the parent Commit.
        You may pass "" as parentCommit in which case the new Commit will have
        no parent and will initially appear empty.
        * description: (optional) explanation of the commit for clarity.
        """
        req = proto.StartCommitRequest(parent=proto.Commit(repo=proto.Repo(name=repo_name), id=parent), branch=branch,
                                       description=description)
        res = self.stub.StartCommit(req, metadata=self.metadata)
        return res

    def finish_commit(self, commit):
        """
        Ends the process of committing data to a Repo and persists the
        Commit. Once a Commit is finished the data becomes immutable and
        future attempts to write to it with PutFile will error.

        Params:
        * commit: A tuple, string, or Commit object representing the commit.
        """
        req = proto.FinishCommitRequest(commit=commit_from(commit))
        res = self.stub.FinishCommit(req, metadata=self.metadata)
        return res

    @contextmanager
    def commit(self, repo_name, branch=None, parent=None, description=None):
        """A context manager for doing stuff inside a commit."""
        commit = self.start_commit(repo_name, branch, parent, description)
        try:
            yield commit
        except Exception as e:
            print("An exception occurred during an open commit. "
                  "Trying to finish it (Currently a commit can't be cancelled)")
            raise e
        finally:
            self.finish_commit(commit)

    def inspect_commit(self, commit):
        """
        Returns info about a specific Commit.

        Params:
        * commit: A tuple, string, or Commit object representing the commit.
        """
        req = proto.InspectCommitRequest(commit=commit_from(commit))
        return self.stub.InspectCommit(req, metadata=self.metadata)

    def provenances_for_repo(self, repo_name):
        provenances = {}
        commits = self.list_commit(repo_name)
        sorted_commits = [x[0] for x in
                          sorted([(c.commit.id, c.finished.seconds) for c in commits], key=lambda x: x[1])]
        for c in sorted_commits:
            for p in c.provenance:
                provenances[p.id] = c.commit.id
        return provenances

    def list_commit(self, repo_name, to_commit=None, from_commit=None, number=0):
        """
        Gets a list of CommitInfo objects.

        Params:
        * repo_name: If only `repo_name` is given, all commits in the repo are
        returned.
        * to_commit: Optional. Only the ancestors of `to`, including `to`
        itself, are considered.
        * from_commit: Optional. Only the descendants of `from`, including
        `from` itself, are considered.
        * number: Optional. Determines how many commits are returned.  If
        `number` is 0, all commits that match the aforementioned criteria are
        returned.
        """
        req = proto.ListCommitRequest(repo=proto.Repo(name=repo_name), number=number)
        if to_commit is not None:
            req.to.CopyFrom(commit_from(to_commit))
        if from_commit is not None:
            getattr(req, 'from').CopyFrom(commit_from(from_commit))
        res = self.stub.ListCommit(req, metadata=self.metadata)
        if hasattr(res, 'commit_info'):
            return res.commit_info
        return []

    def delete_commit(self, commit):
        """
        Deletes a commit.

        Params:
        * commit: A tuple, string, or Commit object representing the commit.
        """
        req = proto.DeleteCommitRequest(commit=commit_from(commit))
        self.stub.DeleteCommit(req, metadata=self.metadata)

    def flush_commit(self, commits, repos=tuple()):
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

        Params:
        * commits: A commit or a list of commits to wait on.
        * repos: Optional. Only the commits up to and including those repos.
        will be considered, otherwise all repos are considered.
        """
        req = proto.FlushCommitRequest(commits=[commit_from(c) for c in commits],
                                       to_repos=[proto.Repo(name=r) for r in repos])
        res = self.stub.FlushCommit(req, metadata=self.metadata)

        for commit in res:
            yield commit

    def subscribe_commit(self, repo_name, branch, from_commit_id=None):
        """
        SubscribeCommit is like ListCommit but it keeps listening for commits as
        they come in. This returns an iterator Commit objects.

        Params:
        * repo_name: Name of the repo.
        * branch: Branch to subscribe to.
        * from_commit_id: Optional. Only commits created since this commit
        are returned.
        """
        repo = proto.Repo(name=repo_name)
        req = proto.SubscribeCommitRequest(repo=repo, branch=branch)
        if from_commit_id is not None:
            getattr(req, 'from').CopyFrom(proto.Commit(repo=repo, id=from_commit_id))
        res = self.stub.SubscribeCommit(req, metadata=self.metadata)
        return res

    def list_branch(self, repo_name):
        """
        Lists the active Branch objects on a Repo.

        Params:
        * repo_name: The name of the repo.
        """
        req = proto.ListBranchRequest(repo=proto.Repo(name=repo_name))
        res = self.stub.ListBranch(req, metadata=self.metadata)
        if hasattr(res, 'branch_info'):
            return res.branch_info
        return []

    def set_branch(self, commit, branch_name):
        """
        Sets a commit and its ancestors as a branch.

        Params:
        * commit: A tuple, string, or Commit object representing the commit.
        * branch_name: The name for the branch to set.
        """
        res = proto.SetBranchRequest(commit=commit_from(commit), branch=branch_name)
        self.stub.SetBranch(res, metadata=self.metadata)

    def delete_branch(self, repo_name, branch_name):
        """
        Deletes a branch, but leaves the commits themselves intact. In other
        words, those commits can still be accessed via commit IDs and other
        branches they happen to be on.

        Params:
        * repo_name: The name of the repo.
        * branch_name: The name of the branch to delete.
        """
        res = proto.DeleteBranchRequest(repo=Repo(name=repo_name), branch=branch_name)
        self.stub.DeleteBranch(res, metadata=self.metadata)

    def put_file_bytes(self, commit, path, value, delimiter=proto.NONE,
                       target_file_datums=0, target_file_bytes=0, overwrite_index=None):
        """
        Uploads a binary bytes array as file(s) in a certain path.

        Params:
        * commit: A tuple, string, or Commit object representing the commit.
        * path: Path in the repo the file(s) will be written to.
        * value: The file contents as bytes, represented as a file-like
        object, bytestring, or iterator of bytestrings.
        * delimiter: Optional. causes data to be broken up into separate files
        with `path` as a prefix.
        * target_file_datums: Optional. Specifies the target number of datums
        in each written file. It may be lower if data does not split evenly,
        but will never be higher, unless the value is 0.
        * target_file_bytes: Specifies the target number of bytes in each
        written file, files may have more or fewer bytes than the target.
        """

        overwrite_index_proto = proto.OverwriteIndex(index=overwrite_index) if overwrite_index is not None else None

        if hasattr(value, "read"):
            def wrap(value):
                for i in itertools.count():
                    chunk = value.read(BUFFER_SIZE)

                    if len(chunk) == 0:
                        return

                    if i == 0:
                        yield proto.PutFileRequest(
                            file=proto.File(commit=commit_from(commit), path=path),
                            value=chunk,
                            delimiter=delimiter,
                            target_file_datums=target_file_datums,
                            target_file_bytes=target_file_bytes,
                            overwrite_index=overwrite_index_proto
                        )
                    else:
                        yield proto.PutFileRequest(value=chunk)
        elif isinstance(value, collections.Iterable) and not isinstance(value, (six.string_types, six.binary_type)):
            def wrap(value):
                for i, chunk in enumerate(value):
                    if i == 0:
                        yield proto.PutFileRequest(
                            file=proto.File(commit=commit_from(commit), path=path),
                            value=chunk,
                            delimiter=delimiter,
                            target_file_datums=target_file_datums,
                            target_file_bytes=target_file_bytes,
                            overwrite_index=overwrite_index_proto
                        )
                    else:
                        yield proto.PutFileRequest(value=chunk)
        else:
            def wrap(value):
                yield proto.PutFileRequest(
                    file=proto.File(commit=commit_from(commit), path=path),
                    value=value[:BUFFER_SIZE],
                    delimiter=delimiter,
                    target_file_datums=target_file_datums,
                    target_file_bytes=target_file_bytes,
                    overwrite_index=overwrite_index_proto
                )

                for i in range(BUFFER_SIZE, len(value), BUFFER_SIZE):
                    yield proto.PutFileRequest(
                        value=value[i:i + BUFFER_SIZE],
                        overwrite_index=overwrite_index_proto
                    )

        self.stub.PutFile(wrap(value), metadata=self.metadata)

    def put_file_url(self, commit, path, url, recursive=False):
        """
        Puts a file using the content found at a URL. The URL is sent to the
        server which performs the request.

        Params:
        * commit: A tuple, string, or Commit object representing the commit.
        * path: The path to the file.
        * url: The url of the file to put.
        * recursive: allow for recursive scraping of some types URLs for
        example on s3:// urls.
        """
        req = iter([
            proto.PutFileRequest(
                file=proto.File(commit=commit_from(commit), path=path),
                url=url,
                recursive=recursive
            )
        ])
        self.stub.PutFile(req, metadata=self.metadata)

    def get_file(self, commit, path, offset_bytes=0, size_bytes=0, extract_value=True):
        """
        Returns an iterator of the contents contents of a file at a specific Commit.

        Params:
        * commit: A tuple, string, or Commit object representing the commit.
        * path: The path of the file.
        * offset_bytes: Optional. specifies a number of bytes that should be
        skipped in the beginning of the file.
        * size_bytes: Optional. limits the total amount of data returned, note
        you will get fewer bytes than size if you pass a value larger than the
        size of the file. If size is set to 0 then all of the data will be
        returned.
        * extract_value: If True, then an ExtractValueIterator will be return,
        which will iterate over the bytes of the file. If False, then the
        protobuf response iterator will return.
        """
        req = proto.GetFileRequest(
            file=proto.File(commit=commit_from(commit), path=path),
            offset_bytes=offset_bytes,
            size_bytes=size_bytes
        )
        res = self.stub.GetFile(req, metadata=self.metadata)
        if extract_value:
            return ExtractValueIterator(res)
        return res

    def get_files(self, commit, paths, recursive=False):
        """
        Returns the contents of a list of files at a specific Commit as a
        dictionary of file paths to data.

        Params:
        * commit: A tuple, string, or Commit object representing the commit.
        * paths: A list of paths to retrieve.
        * recursive: If True, will go into each directory in the list
        recursively.
        """
        filtered_file_infos = []
        for path in paths:
            fi = self.inspect_file(commit, path)
            if fi.file_type == proto.FILE:
                filtered_file_infos.append(fi)
            else:
                filtered_file_infos += self.list_file(commit, path, recursive=recursive)

        filtered_paths = [fi.file.path for fi in filtered_file_infos if fi.file_type == proto.FILE]

        return {path: b''.join(self.get_file(commit, path)) for path in filtered_paths}

    def inspect_file(self, commit, path):
        """
        Returns info about a specific file.

        Params:
        * commit: A tuple, string, or Commit object representing the commit.
        * path: Path to file.
        """
        req = proto.InspectFileRequest(file=proto.File(commit=commit_from(commit), path=path))
        res = self.stub.InspectFile(req, metadata=self.metadata)
        return res

    def list_file(self, commit, path, recursive=False):
        """
        Lists the files in a directory.

        Params:
        * commit: A tuple, string, or Commit object representing the commit.
        * path: The path to the directory.
        * recursive: If True, continue listing the files for sub-directories.
        """
        req = proto.ListFileRequest(
            file=proto.File(commit=commit_from(commit), path=path)
        )
        res = self.stub.ListFile(req, metadata=self.metadata)
        file_infos = res.file_info

        if recursive:
            dirs = [f for f in file_infos if f.file_type == proto.DIR]
            files = [f for f in file_infos if f.file_type == proto.FILE]
            return sum([self.list_file(commit, d.file.path, recursive) for d in dirs], files)

        return list(file_infos)

    def glob_file(self, commit, pattern):
        req = proto.GlobFileRequest(commit=commit_from(commit), pattern=pattern)
        res = self.stub.GlobFile(req, metadata=self.metadata)
        if hasattr(res, 'file_info'):
            return res.file_info
        return []

    def delete_file(self, commit, path):
        """
        Deletes a file from a Commit. DeleteFile leaves a tombstone in the
        Commit, assuming the file isn't written to later attempting to get the
        file from the finished commit will result in not found error. The file
        will of course remain intact in the Commit's parent.

        Params:
        * commit: A tuple, string, or Commit object representing the commit.
        * path: The path to the file.
        """
        req = proto.DeleteFileRequest(file=proto.File(commit=commit_from(commit), path=path))
        self.stub.DeleteFile(req, metadata=self.metadata)

    def delete_all(self):
        req = proto.google_dot_protobuf_dot_empty__pb2.Empty()
        self.stub.DeleteAll(req, metadata=self.metadata)
