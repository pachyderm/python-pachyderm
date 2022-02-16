#!/usr/bin/env python

"""Tests PFS-related functionality"""
from grpclib.exceptions import GRPCError
import pytest

from python_pachyderm.experimental import Client as ExperimentalClient
from python_pachyderm.experimental.mixin.pfs import CreateRepoRequest, Repo
from python_pachyderm.experimental.mixin.transaction import TransactionRequest
from . import util


def test_batch_transaction():
    client = ExperimentalClient()
    expected_repo_count = len(list(client.pfs.list_repo())) + 3

    def create_repo_request():
        repo = Repo(name=util.test_repo_name("test_batch_transaction"))
        return TransactionRequest(create_repo=CreateRepoRequest(repo=repo))

    client.transaction.batch(
        requests=[
            create_repo_request(),
            create_repo_request(),
            create_repo_request(),
        ]
    )

    assert len(client.transaction.list()) == 0
    assert len(list(client.pfs.list_repo())) == expected_repo_count


def test_transaction_context_mgr():
    client = ExperimentalClient()
    expected_repo_count = len(list(client.pfs.list_repo())) + 2

    with client.transaction.transaction() as tx:
        util.create_test_repo(client, "test_transaction_context_mgr")
        util.create_test_repo(client, "test_transaction_context_mgr")

        transactions = client.transaction.list()
        assert len(transactions) == 1
        assert transactions[0].transaction.id == tx.id
        assert client.transaction.inspect(tx).transaction.id == tx.id
        assert client.transaction.inspect(tx.id).transaction.id == tx.id

    assert len(client.transaction.list()) == 0
    assert len(list(client.pfs.list_repo())) == expected_repo_count


def test_transaction_context_mgr_nested():
    client = ExperimentalClient()

    with client.transaction.transaction():
        assert client.transaction.id is not None
        old_transaction_id = client.transaction.id

        with client.transaction.transaction():
            assert client.transaction.id is not None
            assert client.transaction.id != old_transaction_id

        assert client.transaction.id == old_transaction_id


def test_transaction_context_mgr_exception():
    client = ExperimentalClient()
    expected_repo_count = len(list(client.pfs.list_repo()))

    with pytest.raises(Exception):
        with client.transaction.transaction():
            util.create_test_repo(client, "test_transaction_context_mgr_exception")
            util.create_test_repo(client, "test_transaction_context_mgr_exception")
            raise Exception("oops!")

    assert len(client.transaction.list()) == 0
    assert len(list(client.pfs.list_repo())) == expected_repo_count


def test_delete_transaction():
    client = ExperimentalClient()
    expected_repo_count = len(list(client.pfs.list_repo()))

    transaction = client.transaction.start()
    util.create_test_repo(client, "test_delete_transaction")
    util.create_test_repo(client, "test_delete_transaction")
    client.transaction.delete(transaction)

    assert len(client.transaction.list()) == 0
    # even though the transaction was deleted, the repos were still created,
    # because the transaction wasn't tied to the client
    assert len(list(client.pfs.list_repo())) == expected_repo_count + 2

    with pytest.raises(GRPCError):
        # re-deleting should cause an error
        client.transaction.delete(transaction)


def test_delete_all_transactions():
    client = ExperimentalClient()
    client.transaction.start()
    client.transaction.start()
    assert len(client.transaction.list()) == 2
    client.transaction.delete_all()
    assert len(client.transaction.list()) == 0
