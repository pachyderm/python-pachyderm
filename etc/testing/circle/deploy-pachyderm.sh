#!/bin/bash

set -ex

export PATH=$(pwd):$(pwd)/cached-deps:$GOPATH/bin:$PATH

echo 'y' | pachctl deploy local
kubectl wait --for=condition=available deployment -l app=pachd --timeout=5m
pachctl version
