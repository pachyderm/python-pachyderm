#!/usr/bin/env python

"""Tests PFS-related functionality"""

import pytest

import python_pachyderm
from python_pachyderm.proto.v2.pfs import pfs_pb2
from python_pachyderm.proto.v2.transaction import transaction_pb2
from tests import util


def test_batch_transaction():
    client = python_pachyderm.Client()
    expected_repo_count = len(list(client.list_repo())) + 3

    def create_repo_request():
        return transaction_pb2.TransactionRequest(
            create_repo=pfs_pb2.CreateRepoRequest(
                repo=pfs_pb2.Repo(name=util.test_repo_name("test_batch_transaction"))
            )
        )

    client.batch_transaction(
        [
            create_repo_request(),
            create_repo_request(),
            create_repo_request(),
        ]
    )

    assert len(client.list_transaction()) == 0
    assert len(list(client.list_repo())) == expected_repo_count


def test_transaction_context_mgr():
    client = python_pachyderm.Client()
    expected_repo_count = len(list(client.list_repo())) + 2

    with client.transaction() as transaction:
        util.create_test_repo(client, "test_transaction_context_mgr")
        util.create_test_repo(client, "test_transaction_context_mgr")

        transactions = client.list_transaction()
        assert len(transactions) == 1
        assert transactions[0].transaction.id == transaction.id
        assert client.inspect_transaction(transaction).transaction.id == transaction.id
        assert (
            client.inspect_transaction(transaction.id).transaction.id == transaction.id
        )

    assert len(client.list_transaction()) == 0
    assert len(list(client.list_repo())) == expected_repo_count


def test_transaction_context_mgr_nested():
    client = python_pachyderm.Client()

    with client.transaction():
        assert client.transaction_id is not None
        old_transaction_id = client.transaction_id

        with client.transaction():
            assert client.transaction_id is not None
            assert client.transaction_id != old_transaction_id

        assert client.transaction_id == old_transaction_id


def test_transaction_context_mgr_exception():
    client = python_pachyderm.Client()
    expected_repo_count = len(list(client.list_repo()))

    with pytest.raises(Exception):
        with client.transaction():
            util.create_test_repo(client, "test_transaction_context_mgr_exception")
            util.create_test_repo(client, "test_transaction_context_mgr_exception")
            raise Exception("oops!")

    assert len(client.list_transaction()) == 0
    assert len(list(client.list_repo())) == expected_repo_count


def test_delete_transaction():
    client = python_pachyderm.Client()
    expected_repo_count = len(list(client.list_repo()))

    transaction = client.start_transaction()
    util.create_test_repo(client, "test_delete_transaction")
    util.create_test_repo(client, "test_delete_transaction")
    client.delete_transaction(transaction)

    assert len(client.list_transaction()) == 0
    # even though the transaction was deleted, the repos were still created,
    # because the transaction wasn't tied to the client
    assert len(list(client.list_repo())) == expected_repo_count + 2

    with pytest.raises(python_pachyderm.RpcError):
        # re-deleting should cause an error
        client.delete_transaction(transaction)


def test_delete_all_transactions():
    client = python_pachyderm.Client()
    client.start_transaction()
    client.start_transaction()
    assert len(client.list_transaction()) == 2
    client.delete_all_transactions()
    assert len(client.list_transaction()) == 0
