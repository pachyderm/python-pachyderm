#!/usr/bin/env python

"""Tests admin-related functionality"""

import os
from contextlib import contextmanager
from time import sleep

import pytest

import python_pachyderm
from tests import util


@contextmanager
def sandbox():
    client = python_pachyderm.Client()
    client.activate_enterprise(os.environ["PACH_PYTHON_ENTERPRISE_CODE"])
    root_auth_token = None

    try:
        root_auth_token = client.activate_auth("robot:root")
        client.auth_token = root_auth_token
        try:
            yield client
        finally:
            try:
                client.deactivate_auth()
                client.auth_token = None
            except:
                print(
                    "an exception occurred trying to deactivate auth, please manually disable auth with the root auth token: {}".format(
                        root_auth_token
                    )
                )
                raise
    finally:
        client.deactivate_enterprise()


@util.skip_if_no_enterprise()
def test_auth_configuration():
    with sandbox() as client:
        config = client.get_auth_configuration()
        client.set_auth_configuration(config)


@util.skip_if_no_enterprise()
def test_admins():
    with sandbox() as client:
        users = client.get_admins()
        assert users == ["robot:root"]
        client.modify_admins(add=["robot:someuser"])

        # Retry this check three times, in case admin cache is slow
        expected = set(["robot:root", "robot:someuser"])
        for i in range(3):
            users = client.get_admins()
            if set(users) == expected:
                return  # success
            sleep(3)
        print("expected admins {} but got {}".format(expected, set(users)))
        raise


@util.skip_if_no_enterprise()
def test_one_time_password():
    with sandbox() as client:
        otp = client.get_one_time_password()
        client.authenticate_one_time_password(otp)


@util.skip_if_no_enterprise()
def test_authorize():
    with sandbox() as client:
        assert client.authorize("foobar", python_pachyderm.Scope.READER.value)


@util.skip_if_no_enterprise()
def test_who_am_i():
    with sandbox() as client:
        i = client.who_am_i()
        assert i.username == "robot:root"
        assert i.is_admin


@util.skip_if_no_enterprise()
def test_scope():
    with sandbox() as client:
        repo = util.create_test_repo(client, "test_scope")
        scopes = client.get_scope("robot:root", repo)
        assert all(s == python_pachyderm.Scope.NONE.value for s in scopes)
        client.set_scope("robot:root", repo, python_pachyderm.Scope.READER.value)
        scopes = client.get_scope("robot:root", repo)
        assert all(s == python_pachyderm.Scope.NONE.value for s in scopes)


@util.skip_if_no_enterprise()
def test_acl():
    def verify_acl(client, repo):
        acl = client.get_acl(repo)
        assert len(acl.entries) == 1
        assert len(acl.robot_entries) == 0
        assert acl.entries[0].username == "robot:root"
        assert acl.entries[0].scope == python_pachyderm.Scope.OWNER.value
        return acl

    with sandbox() as client:
        repo = util.create_test_repo(client, "test_acl")
        acl = verify_acl(client, repo)
        client.set_acl(repo, acl.entries)
        verify_acl(client, repo)


@util.skip_if_no_enterprise()
def test_auth_token():
    with sandbox() as client:
        auth_token = client.get_auth_token("robot:root", ttl=30)
        assert auth_token.subject == "robot:root"
        client.extend_auth_token(auth_token.token, 60)
        client.revoke_auth_token(auth_token.token)
        with pytest.raises(python_pachyderm.RpcError):
            client.extend_auth_token(auth_token.token, 60)


@util.skip_if_no_enterprise()
def test_groups():
    with sandbox() as client:
        assert client.get_groups() == []
        client.set_groups_for_user("robot:root", ["foogroup"])
        assert client.get_groups() == ["foogroup"]
        assert client.get_users("foogroup") == ["robot:root"]
        client.modify_members("foogroup", remove=["robot:root"])
        assert client.get_groups() == []
        assert client.get_users("foogroup") == []
