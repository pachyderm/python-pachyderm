#!/usr/bin/env python

"""Tests for health-related functionality."""

import python_pachyderm


def test_health():
    foo = python_pachyderm.experimental.Client().health_check()
    print(foo)
