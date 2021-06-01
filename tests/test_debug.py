#!/usr/bin/env python

"""Tests debug-related functionality"""
import pytest

import python_pachyderm


def test_dump():
    client = python_pachyderm.Client()
    for b in client.dump():
        assert isinstance(b, bytes)
        assert len(b) > 0


def test_profile_cpu():
    client = python_pachyderm.Client()
    for b in client.profile_cpu(python_pachyderm.Duration(seconds=1)):
        assert isinstance(b, bytes)
        assert len(b) > 0


@pytest.mark.skip(reason="RESOURCE_EXHAUSTED error: Received message larger than max")
def test_binary():
    client = python_pachyderm.Client()
    # TODO why does this fail?
    for b in client.binary():
        assert isinstance(b, bytes)
        assert len(b) > 0
