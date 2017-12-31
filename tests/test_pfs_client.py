#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `pypachy` package."""

import pytest


import pypachy


def test_pfs_list_repo():
    # GIVEN a Pachyderm deployment
    # WHEN a client is created
    client = pypachy.PfsClient()
    # THEN list_repo() should return an empty list
    assert list(client.list_repo()) == []
