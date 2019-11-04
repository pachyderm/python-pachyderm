#!/usr/bin/env python

"""Tests PFS-related functionality"""

import pytest
import random
import string
import threading
from io import BytesIO
from collections import namedtuple

import python_pachyderm
from tests import util


@util.skip_if_below_pachyderm_version(1, 9, 0)
def test_transaction_context_mgr():
    client = python_pachyderm.Client()
    expected_repo_count = len(client.list_repo()) + 2

    with client.transaction() as transaction:
        util.create_test_repo(client, "test_transaction_context_mgr")
        util.create_test_repo(client, "test_transaction_context_mgr")

        transactions = client.list_transaction()
        assert len(transactions) == 1
        assert transactions[0].transaction.id == transaction.id
        assert client.inspect_transaction(transaction).transaction.id == transaction.id
        assert client.inspect_transaction(transaction.id).transaction.id == transaction.id

    assert len(client.list_transaction()) == 0
    assert len(client.list_repo()) == expected_repo_count

@util.skip_if_below_pachyderm_version(1, 9, 0)
def test_transaction_context_mgr_arg():
    client = python_pachyderm.Client()
    expected_repo_count = len(client.list_repo()) + 2

    with client.transaction(client.start_transaction()) as transaction:
        util.create_test_repo(client, "test_transaction_context_mgr_arg")
        util.create_test_repo(client, "test_transaction_context_mgr_arg")

    assert len(client.list_transaction()) == 0
    assert len(client.list_repo()) == expected_repo_count

@util.skip_if_below_pachyderm_version(1, 9, 0)
def test_transaction_context_mgr_nested():
    with pytest.raises(Exception):
        with client.transaction() as transaction:
            # it shouldn't be possible to nest transactions
            with client.transaction() as transaction:
                pass

@util.skip_if_below_pachyderm_version(1, 9, 0)
def test_transaction_context_mgr_exception():
    client = python_pachyderm.Client()
    expected_repo_count = len(client.list_repo())

    with pytest.raises(Exception):
        with client.transaction() as transaction:
            util.create_test_repo(client, "test_transaction_context_mgr_exception")
            util.create_test_repo(client, "test_transaction_context_mgr_exception")
            raise Exception("oops!")

    assert len(client.list_transaction()) == 0
    assert len(client.list_repo()) == expected_repo_count

@util.skip_if_below_pachyderm_version(1, 9, 0)
def test_delete_transaction():
    client = python_pachyderm.Client()
    expected_repo_count = len(client.list_repo())

    transaction = client.start_transaction()
    util.create_test_repo(client, "test_delete_transaction")
    util.create_test_repo(client, "test_delete_transaction")
    client.delete_transaction(transaction)

    assert len(client.list_transaction()) == 0
    # even though the transaction was deleted, the repos were still created,
    # because the transaction wasn't tied to the client
    assert len(client.list_repo()) == expected_repo_count + 2

    with pytest.raises(Exception):
        # re-deleting should cause an error
        client.delete_transaction(transaction)

@util.skip_if_below_pachyderm_version(1, 9, 0)
def test_delete_all_transactions():
    client = python_pachyderm.Client()
    client.start_transaction()
    client.start_transaction()
    assert len(client.list_transaction()) == 2
    client.delete_all_transactions()
    assert len(client.list_transaction()) == 0
