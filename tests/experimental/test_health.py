#!/usr/bin/env python

"""Tests for health-related functionality."""

import python_pachyderm


def test_health():
    python_pachyderm.experimental.Client().health_check()
