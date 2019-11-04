#!/usr/bin/env python

"""Tests for health-related functionality."""

import python_pachyderm


def test_health():
    python_pachyderm.Client().health()
