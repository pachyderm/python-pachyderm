# -*- coding: utf-8 -*-

import collections
import os
from builtins import object
from contextlib import contextmanager

from .proto.client.pfs.pfs_pb2 import *
from .proto.client.pfs.pfs_pb2_grpc import *

BUFFER_SIZE = 3 * 1024 * 1024  # 3MB TODO: Base this on some grpc value


class ExtractValueIterator(object):
    def __init__(self, r):
        self._iter = r

    def __next__(self):
        return next(self._iter).value

    def __iter__(self):
        return self


def _commit_from(src, allow_just_repo=False):
    if src.__class__.__name__ == "Commit":
        return src
    elif type(src) in (tuple, list) and len(src) == 2:
        return Commit(repo=Repo(name=src[0]), id=src[1])
    elif type(src) is str:
        repo_name, commit_id = src.split('/', 1)
        return Commit(repo=Repo(name=repo_name), id=commit_id)
    if not allow_just_repo:
        raise ValueError(
            "Commit should either be a sequence of [repo, commit_id] or a string in the form 'repo/branch/commit_id")
    return Commit(repo=Repo(name=src))


def _make_list(x):
    # if `x` is not iterable, put it in a list
    if isinstance(x, (str, bytes)) or not isinstance(x, collections.Iterable):
        x = [x]
    return x


def _is_iterator(x):
    return hasattr(x, 'next') or hasattr(x, '__next__')


class PfsClient(object):
    def __init__(self, host=None, port=None):
        """
        Creates a client to connect to Pfs
        :param host: The pachd host. Default is 'localhost', which is used with `pachctl port-forward`
        :param port: The port to connect to. Default is 30650
        """
        # If a host or port is not specified, then try to set using environment variables or use the defaults.
        if host is None:
            host = os.environ.get('PACHD_SERVICE_HOST', 'localhost')
        if port is None:
            port = os.environ.get('PACHD_SERVICE_PORT_API_GRPC_PORT', '30650')

        self.channel = grpc.insecure_channel('{}:{}'.format(host, port))
        self.stub = APIStub(self.channel)

    def create_repo(self, repo_name, description=None):
        """
        Creates a new Repo object in pfs with the given name. Repos are
        the top level data object in pfs and should be used to store data of a
        similar type. For example rather than having a single Repo for an entire
        project you might have seperate Repos for logs, metrics, database dumps etc.
        :param repo_name: Name of the repo
        :param description: Repo description
        """
        self.stub.CreateRepo(CreateRepoRequest(repo=Repo(name=repo_name), description=description))

    def inspect_repo(self, repo_name):
        """
        returns info about a specific Repo.
        :param repo_name: Name of the repo
        :return: A RepoInfo object
        """
        return self.stub.InspectRepo(InspectRepoRequest(repo=Repo(name=repo_name)))

    def list_repo(self, provenance=tuple()):
        """
        Returns info about all Repos.

        :param provenance: Optional. Specifies a set of provenance repos where only repos which have ALL of
        the specified repos as provenance will be returned.
        :return: A list of RepoInfo objects
        """
        x = self.stub.ListRepo(ListRepoRequest(provenance=[Repo(name=p) for p in provenance]))
        if hasattr(x, 'repo_info'):
            return x.repo_info
        return []

    def delete_repo(self, repo_name=None, force=False, all=False):
        """
        Deletes a repo and reclaims the storage space it was using. Note
        that as of 1.0 we do not reclaim the blocks that the Repo was referencing,
        this is because they may also be referenced by other Repos and deleting them
        would make those Repos inaccessible. This will be resolved in later
        versions.
        :param repo_name: The name of the repo
        :param force: if set to true, the repo will be removed regardless of errors.
                      This argument should be used with care.
        :param all: Delete all repos
        """
        if not all:
            if repo_name:
                self.stub.DeleteRepo(DeleteRepoRequest(repo=Repo(name=repo_name), force=force))
            else:
                raise ValueError("Either a repo_name or all=True needs to be provided")
        else:
            if not repo_name:
                self.stub.DeleteRepo(DeleteRepoRequest(force=force, all=all))
            else:
                raise ValueError("Cannot specify a repo_name if all=True")

    def start_commit(self, repo_name, branch=None, parent=None):
        """
        Begins the process of committing data to a Repo. Once started
        you can write to the Commit with PutFile and when all the data has been
        written you must finish the Commit with FinishCommit. NOTE, data is not
        persisted until FinishCommit is called.
        :param repo_name: The name of the repo
        :param branch: is a more convenient way to build linear chains of commits. When a
                    commit is started with a non empty branch the value of branch becomes an
                    alias for the created Commit. This enables a more intuitive access pattern.
                    When the commit is started on a branch the previous head of the branch is
                    used as the parent of the commit.
        :param parent: specifies the parent Commit, upon creation the new Commit will
                    appear identical to the parent Commit, data can safely be added to the new
                    commit without affecting the contents of the parent Commit. You may pass ""
                    as parentCommit in which case the new Commit will have no parent and will
                    initially appear empty.
        :return: Commit object
        """
        return self.stub.StartCommit(StartCommitRequest(parent=Commit(repo=Repo(name=repo_name),
                                                                      id=parent),
                                                        branch=branch))

    def finish_commit(self, commit):
        """
        Ends the process of committing data to a Repo and persists the
        Commit. Once a Commit is finished the data becomes immutable and future
        attempts to write to it with PutFile will error.
        :param commit: A tuple or string representing the commit
        """
        self.stub.FinishCommit(FinishCommitRequest(commit=_commit_from(commit)))

    @contextmanager
    def commit(self, repo_name, branch=None, parent=None):
        """
        A context manager for doing stuff inside a commit
        """
        commit = self.start_commit(repo_name, branch, parent)
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
        returns info about a specific Commit.
        :param commit: A tuple or string representing the commit
        :return: CommitInfo object
        """
        return self.stub.InspectCommit(InspectCommitRequest(commit=_commit_from(commit)))

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
        Lists commits.
         
        :param repo_name: If only `repo_name` is given, all commits in the repo are returned.
        :param to_commit: optional. only the ancestors of `to`, including `to` itself,
                        are considered.
        :param from_commit: optional. only the descendents of `from`, including `from`
                        itself, are considered.
        :param number: optional. determines how many commits are returned.  If `number` is 0,
                    all commits that match the aforementioned criteria are returned.
        :return: A list of CommitInfo objects
        """
        req = ListCommitRequest(repo=Repo(name=repo_name), number=number)
        if to_commit is not None:
            req.to.CopyFrom(_commit_from(to_commit))
        if from_commit is not None:
            getattr(req, 'from').CopyFrom(_commit_from(from_commit))
        x = self.stub.ListCommit(req)
        if hasattr(x, 'commit_info'):
            return x.commit_info
        return []

    def delete_commit(self, commit):
        """
        deletes a commit.
        Note it is currently not implemented.
        :param commit: A tuple or string representing the commit
        """
        self.stub.DeleteCommit(DeleteCommitRequest(commit=_commit_from(commit)))

    def flush_commit(self, commits, repos=tuple()):
        """
        blocks until all of the commits which have a set of commits as
        provenance have finished. For commits to be considered they must have all of
        the specified commits as provenance. This in effect waits for all of the
        jobs that are triggered by a set of commits to complete.
        It returns an error if any of the commits it's waiting on are cancelled due
        to one of the jobs encountering an error during runtime.
        Note that it's never necessary to call FlushCommit to run jobs, they'll run
        no matter what, FlushCommit just allows you to wait for them to complete and
        see their output once they do.
        :param commits: A commit or a list of commits to wait on
        :param repos: Optional. Only the commits up to and including those repos
                    will be considered, otherwise all repos are considered.
        :return: An iterator of CommitInfo objects
        """
        return self.stub.FlushCommit(FlushCommitRequest(commit=[_commit_from(c) for c in commits],
                                                        to_repo=[Repo(name=r) for r in repos]))

    def subscribe_commit(self, repo_name, branch, from_commit_id=None):
        """
        SubscribeCommit is like ListCommit but it keeps listening for commits as
        they come in.

        :param repo_name: Name of the repo
        :param branch: Branch to subscribe to 
        :param from_commit_id: Optional. only commits created since this commit are returned

        :return: Iterator of Commit objects
        """
        repo = Repo(name=repo_name)
        req = SubscribeCommitRequest(repo=repo, branch=branch)
        if from_commit_id is not None:
            getattr(req, 'from').CopyFrom(Commit(repo=repo, id=from_commit_id))
        return self.stub.SubscribeCommit(req)

    def list_branch(self, repo_name):
        """
        lists the active branches on a Repo
        :param repo_name: The name of the repo
        :return: A list of Branch objects
        """
        x = self.stub.ListBranch(ListBranchRequest(repo=Repo(name=repo_name)))
        if hasattr(x, 'branch_info'):
            return x.branch_info
        return []

    def set_branch(self, commit, branch_name):
        """
        sets a commit and its ancestors as a branch
        :param commit: A tuple or string representing the commit 
        :param branch_name: The name for the branch to set
        """
        self.stub.SetBranch(SetBranchRequest(commit=_commit_from(commit),
                                             branch=branch_name))

    def delete_branch(self, repo_name, branch_name):
        """
        deletes a branch, but leaves the commits themselves intact.
        In other words, those commits can still be accessed via commit IDs and
        other branches they happen to be on.
        :param repo_name: The name of the repo 
        :param branch_name: The name of the branch to delete 
        """
        self.stub.DeleteBranch(DeleteBranchRequest(repo=Repo(name=repo_name),
                                                   branch=branch_name))

    def put_file_bytes(self, commit, path, value, delimiter=NONE,
                       target_file_datums=0, target_file_bytes=0):
        """
        Uploads a binary bytes array as file(s) in a certain path
        :param commit: A tuple or string representing the commit 
        :param path: Path in the repo the file(s) will be written to
        :param value: The data bytes array, or an iterator returning chunked byte arrays 
        :param delimiter: Optional. causes data to be broken up into separate files with `path`
                as a prefix.
        :param target_file_datums: Optional. specifies the target number of datums in each written
                file it may be lower if data does not split evenly, but will never be
                higher, unless the value is 0.
        :param target_file_bytes: specifies the target number of bytes in each written
                file, files may have more or fewer bytes than the target. 
        """

        if _is_iterator(value):
            def _wrap(v):
                for x in v:
                    yield PutFileRequest(file=File(commit=_commit_from(commit), path=path),
                                         value=x,
                                         delimiter=delimiter,
                                         target_file_datums=target_file_datums,
                                         target_file_bytes=target_file_bytes)

            self.stub.PutFile(_wrap(value))
            return

        def _blocks(v):
            for i in range(0, len(v), BUFFER_SIZE):
                yield PutFileRequest(file=File(commit=_commit_from(commit), path=path),
                                     value=v[i:i + BUFFER_SIZE],
                                     delimiter=delimiter,
                                     target_file_datums=target_file_datums,
                                     target_file_bytes=target_file_bytes)

        self.stub.PutFile(_blocks(value))

    def put_file_url(self, commit, path, url, recursive=False):
        """
        puts a file using the content found at a URL.
        The URL is sent to the server which performs the request.

        :param commit: A tuple or string representing the commit 
        :param path: The path to the file
        :param url: The url to download
        :param recursive: allow for recursive scraping of some types URLs for example on s3:// urls.
        """
        self.stub.PutFile(iter([PutFileRequest(file=File(commit=_commit_from(commit), path=path),
                                               url=url,
                                               recursive=recursive)]))

    def get_file(self, commit, path, offset_bytes=0, size_bytes=0, extract_value=True):
        """
        returns the contents of a file at a specific Commit.

        :param commit: A tuple or string representing the commit 
        :param path: The path of the file
        :param offset_bytes: Optional. specifies a number of bytes that should be skipped in the beginning of the file. 
        :param size_bytes: Optional. limits the total amount of data returned, note you will get fewer bytes
                than size if you pass a value larger than the size of the file.
                If size is set to 0 then all of the data will be returned.
        :param extract_value: If True, then an ExtractValueIterator will be return, which 
                    will iterate over the bytes of the file. If False, then the Protobuf
                    response iterator will return
        :return: An iterator over the file or an iterator over the protobuf responses
        """
        r = self.stub.GetFile(GetFileRequest(file=File(commit=_commit_from(commit),
                                                       path=path),
                                             offset_bytes=offset_bytes,
                                             size_bytes=size_bytes))
        if extract_value:
            return ExtractValueIterator(r)
        return r

    def get_files(self, commit, paths, recursive=False):
        """
        returns the contents of a list of files at a specific Commit.

        :param commit: A tuple or string representing the commit 
        :param paths: A list of paths to retrieve
        :param recursive: If True, will go into each directory in the list recursively
        :return: A dictionary of file paths and data
        """
        filtered_file_infos = []
        for path in paths:
            fi = self.inspect_file(commit, path)
            if fi.file_type == FILE:
                filtered_file_infos.append(fi)
            else:
                filtered_file_infos += self.list_file(commit, path, recursive=recursive)

        filtered_paths = [fi.file.path for fi in filtered_file_infos if fi.file_type == FILE]

        return {path: b''.join(self.get_file(commit, path)) for path in filtered_paths}

    def inspect_file(self, commit, path):
        """
        returns info about a specific file.
        :param commit: A tuple or string representing the commit 
        :param path: Path to file
        :return: A FileInfo object
        """
        return self.stub.InspectFile(InspectFileRequest(file=File(commit=_commit_from(commit),
                                                                  path=path)))

    def list_file(self, commit, path, recursive=False):
        """
        Lists the files in a directory
        :param commit: A tuple or string representing the commit
        :param path: The path to the directory
        :param recursive: If True, continue listing the files for sub-directories
        :return: A list of FileInfo objects
        """
        file_infos = self.stub.ListFile(ListFileRequest(file=File(commit=_commit_from(commit),
                                                                  path=path))).file_info
        if recursive:
            dirs = [f for f in file_infos if f.file_type == DIR]
            files = [f for f in file_infos if f.file_type == FILE]
            return sum([self.list_file(commit, d.file.path, recursive) for d in dirs],
                       files)

        return list(file_infos)

    def glob_file(self, commit, pattern):
        """
        ?
        :param commit: 
        :param pattern: 
        :return: A list of FileInfo objects
        """
        r = self.stub.GlobFile(GlobFileRequest(commit=_commit_from(commit),
                                               pattern=pattern))
        if hasattr(r, 'file_info'):
            return r.file_info
        return []

    def delete_file(self, commit, path):
        """
        deletes a file from a Commit.
        DeleteFile leaves a tombstone in the Commit, assuming the file isn't written
        to later attempting to get the file from the finished commit will result in
        not found error.
        The file will of course remain intact in the Commit's parent.

        :param commit: A tuple or string representing the commit
        :param path: The path to the file
        """
        self.stub.DeleteFile(DeleteFileRequest(file=File(commit=_commit_from(commit),
                                                         path=path)))

    def delete_all(self):
        self.stub.DeleteAll(google_dot_protobuf_dot_empty__pb2.Empty())
