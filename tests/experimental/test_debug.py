#!/usr/bin/env python

"""Tests debug-related functionality"""

import python_pachyderm
from google.protobuf import duration_pb2
import datetime

# bp_to_pb: datetime.timedelta -> duration_pb2.Duration


def test_dump():
    client = python_pachyderm.experimental.Client()
    for b in client.dump():
        assert isinstance(b, bytes)
        assert len(b) > 0


def test_profile_cpu():
    client = python_pachyderm.experimental.Client()
    for b in client.profile_cpu(datetime.timedelta(seconds=1)):
        assert isinstance(b, bytes)
        assert len(b) > 0


def test_binary():
    client = python_pachyderm.experimental.Client()
    for b in client.binary():
        assert isinstance(b, bytes)
        assert len(b) > 0
