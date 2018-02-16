#!/bin/bash
set -euxo pipefail

pachctl deploy local --dry-run | kubectl create -f -
