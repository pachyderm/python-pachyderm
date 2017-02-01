from .pfs_pb2 import *
from .pfs_pb2_grpc import *


def _commit_from(src):
    if type(src) in (tuple, list) and len(src) == 2:
        return Commit(repo=Repo(name=src[0]), id=src[1])
    elif type(src) is str:
        repo_name, commit_id = src.split('/', 1)
        return Commit(repo=Repo(name=repo_name), id=commit_id)
    raise ValueError(
        "Commit should either be a sequence of [repo, commit_id] or a string in the form 'repo/branch/commit_id")


def _diff_method(from_commit_id, repo_name, full_file):
    return None if from_commit_id is None else DiffMethod(from_commit=Commit(repo=Repo(name=repo_name),
                                                                             id=from_commit_id),
                                                          full_file=full_file)


class PfsClient(object):
    def __init__(self, port=30650):
        self.channel = grpc.insecure_channel('localhost:{}'.format(port))
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

    def fork_commit(self, repo_name, parent_commit, branch_name):
        return self.stub.ForkCommit(
            ForkCommitRequest(commit=Commit(repo=Repo(repo_name), id=parent_commit), branch=branch_name))

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

    def list_commit(self, exclude, include, provenance, commit_type, status, block=False):
        return self.stub.ListCommit(exclude=[_commit_from(e) for e in exclude],
                                    include=[_commit_from(i) for i in include],
                                    provenance=[_commit_from(p) for p in provenance],
                                    commit_type=commit_type,
                                    status=status,
                                    block=block)

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

    def get_file(self, repo_name, commit_id, path, from_commit_id=None, full_file=False):

        return self.stub.GetFile(GetFileRequest(file=File(commit=Commit(repo=Repo(name=repo_name), id=commit_id),
                                                          path=path),
                                                offset_bytes=0,
                                                size_bytes=0,
                                                shard=Shard(),
                                                diff_method=_diff_method(from_commit_id, repo_name, full_file)))

    def inspect_file(self, repo_name, commit_id, path, from_commit_id=None, full_file=False):
        return self.stub.InspectFile(InspectFileRequest(file=File(commit=Commit(repo=Repo(name=repo_name), id=commit_id),
                                                                  path=path),
                                                        shard=Shard(),
                                                        diff_method=_diff_method(from_commit_id, repo_name, full_file)))

    def list_file(self, repo_name, commit_id, path, mode=ListFile_NORMAL, from_commit_id=None, full_file=False):
        return self.stub.ListFile(ListFileRequest(file=File(commit=Commit(repo=Repo(name=repo_name), id=commit_id),
                                                            path=path),
                                                  shard=Shard(),
                                                  diff_method=_diff_method(from_commit_id, repo_name, full_file),
                                                  mode=mode))

    def delete_file(self, repo_name, commit_id, path):
        self.stub.DeleteFile(DeleteFileRequest(file=File(commit=Commit(repo=Repo(name=repo_name), id=commit_id),
                                                         path=path)))

    def delete_all(self):
        self.stub.DeleteAll()

    def archive_all(self):
        self.stub.ArchiveAll()
