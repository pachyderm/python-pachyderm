import collections

from .pfs_pb2 import *
from .pfs_pb2_grpc import *


def _commit_from(src, allow_just_repo=False):
    if type(src) in (tuple, list) and len(src) == 2:
        return Commit(repo=Repo(name=src[0]), id=src[1])
    elif type(src) is str:
        repo_name, commit_id = src.split('/', 1)
        return Commit(repo=Repo(name=repo_name), id=commit_id)
    if not allow_just_repo:
        raise ValueError(
            "Commit should either be a sequence of [repo, commit_id] or a string in the form 'repo/branch/commit_id")
    return Commit(repo=Repo(name=src))


def _diff_method(from_commit_id, commit, full_file):
    c = _commit_from(commit)
    return None if from_commit_id is None else DiffMethod(from_commit=Commit(repo=c.repo,
                                                                             id=from_commit_id),
                                                          full_file=full_file)


class PfsClient(object):
    def __init__(self, host="localhost", port=30650):
        self.channel = grpc.insecure_channel('{}:{}'.format(host, port))
        self.stub = APIStub(self.channel)

    def create_repo(self, repo_name):
        self.stub.CreateRepo(CreateRepoRequest(repo=Repo(name=repo_name)))

    def inspect_repo(self, repo_name):
        return self.stub.InspectRepo(InspectRepoRequest(repo=Repo(name=repo_name)))

    def list_repo(self, provenance=tuple()):
        return self.stub.ListRepo(ListRepoRequest(provenance=[Repo(name=p) for p in provenance]))

    def delete_repo(self, repo_name, force=False):
        self.stub.DeleteRepo(DeleteRepoRequest(repo=Repo(name=repo_name), force=force))

    def start_commit(self, repo_name, parent):
        return self.stub.StartCommit(StartCommitRequest(parent=Commit(repo=Repo(name=repo_name), id=parent)))

    def fork_commit(self, commit, branch_name):
        return self.stub.ForkCommit(
            ForkCommitRequest(commit=_commit_from(commit), branch=branch_name))

    def finish_commit(self, commit):
        self.stub.FinishCommit(
            FinishCommitRequest(commit=_commit_from(commit), cancel=False))

    def cancel_commit(self, commit):
        self.stub.FinishCommit(
            FinishCommitRequest(commit=_commit_from(commit), cancel=True))

    def archive_commit(self, commit):
        self.stub.ArchiveCommit(ArchiveCommitRequest(commits=[_commit_from(commit)]))

    def inspect_commit(self, commit):
        return self.stub.InspectCommit(InspectRepoRequest(commit=_commit_from(commit)))

    def list_commit(self, include, exclude=tuple(), provenance=tuple(), commit_type=COMMIT_TYPE_NONE,
                    status=ALL, block=False):
        # if `include` is not iterable, put it in a list
        if isinstance(include, (str, bytes)) or not isinstance(include, collections.Iterable):
            include = [include]
        return self.stub.ListCommit(
            ListCommitRequest(
                exclude=[_commit_from(e, True) for e in exclude],
                include=[_commit_from(i, True) for i in include],
                provenance=[_commit_from(p) for p in provenance],
                commit_type=commit_type,
                status=status,
                block=block))

    def delete_commit(self, commit):
        self.stub.DeleteCommit(DeleteCommitRequest(commit=_commit_from(commit)))

    def flush_commit(self, commits, repos):
        return self.stub.FlushCommit(FlushCommitRequest(commit=[_commit_from(c) for c in commits],
                                                        to_repo=[Repo(name=r) for r in repos]))

    def list_branch(self, repo_name, status):
        return self.stub.ListBranch(ListBranchRequest(repo=Repo(name=repo_name), status=status))

    def squash_commit(self, from_commits, to_commit):
        self.stub.SquashCommit(SquashCommitRequest(from_commits=[_commit_from(c) for c in from_commits],
                                                   to_commit=_commit_from(to_commit)))

    def replay_commit(self, from_commits, to_branch):
        return self.stub.ReplayCommit(ReplayCommitRequest(from_commits=[_commit_from(c) for c in from_commits],
                                                          to_branch=to_branch))

    def put_file(self, commit, path, file_type, value=None, delimiter=LINE, url='', recursive=False):
        self.stub.PutFile(iter([PutFileRequest(file=File(commit=_commit_from(commit), path=path),
                                               file_type=file_type,
                                               value=value,
                                               delimiter=delimiter,
                                               url=url,
                                               recursive=recursive)]))

    def get_file(self, commit, path, from_commit_id=None, full_file=False, get_bytes=True):

        r = self.stub.GetFile(GetFileRequest(file=File(commit=_commit_from(commit),
                                                       path=path),
                                             offset_bytes=0,
                                             size_bytes=0,
                                             shard=Shard(),
                                             diff_method=_diff_method(from_commit_id, commit, full_file)))
        if not get_bytes:
            return r
        return r.next().value

    def get_files(self, commit, paths, from_commit_id=None, full_file=False, get_bytes=True, recursive=False):
        filtered_file_infos = sum([self.list_file(commit, path, from_commit_id=from_commit_id, full_file=full_file,
                                                  recursive=recursive)
                                   for path in paths], [])
        filtered_paths = [fi.file.path for fi in filtered_file_infos if fi.file_type == FILE_TYPE_REGULAR]

        return {path: self.get_file(commit, path, from_commit_id, full_file, get_bytes) for path in filtered_paths}

    def inspect_file(self, commit, path, from_commit_id=None, full_file=False):
        return self.stub.InspectFile(InspectFileRequest(file=File(commit=_commit_from(commit),
                                                                  path=path),
                                                        shard=Shard(),
                                                        diff_method=_diff_method(from_commit_id, commit, full_file)))

    def list_file(self, commit, path, mode=ListFile_NORMAL, from_commit_id=None, full_file=False, recursive=False):
        file_infos = self.stub.ListFile(ListFileRequest(file=File(commit=_commit_from(commit),
                                                                  path=path),
                                                        shard=Shard(),
                                                        diff_method=_diff_method(from_commit_id, commit, full_file),
                                                        mode=mode)).file_info
        if recursive:
            dirs = [f for f in file_infos if f.file_type == FILE_TYPE_DIR]
            files = [f for f in file_infos if f.file_type == FILE_TYPE_REGULAR]
            return sum([self.list_file(commit, d.file.path, mode, from_commit_id, full_file, recursive) for d in dirs],
                       files)

        return list(file_infos)

    def delete_file(self, commit, path):
        self.stub.DeleteFile(DeleteFileRequest(file=File(commit=_commit_from(commit),
                                                         path=path)))

    def delete_all(self):
        self.stub.DeleteAll()

    def archive_all(self):
        self.stub.ArchiveAll()
