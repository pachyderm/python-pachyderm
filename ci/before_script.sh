#!/bin/bash

# -e : Readline  is used to obtain the line
# -x : Print a trace of commands and their arguments after they are expanded and before they are executed
set -ex

# Start minikube
sudo CHANGE_MINIKUBE_NONE_USER=true minikube start --vm-driver=none --kubernetes-version=v1.8.0

# Fix the kubectl context in case it is stale
minikube update-context

# Wait for Kubernetes to be up and ready
JSONPATH='{range .items[*]}{@.metadata.name}:{range @.status.conditions[*]}{@.type}={@.status};{end}{end}'; until kubectl get nodes -o jsonpath="$JSONPATH" 2>&1 | grep -q "Ready=True"; do sleep 1; done

# Deploy Pachyderm
pachctl deploy local

# Wait for both Pachyderm pods to be ready
JSONPATH='{range .items[*]}{@.metadata.name}:{range @.status.conditions[*]}{@.type}={@.status};{end}{end}'; until kubectl get pods -l app=pachd -o jsonpath="$JSONPATH" 2>&1 | grep -q "Ready=True"; do sleep 1; done
JSONPATH='{range .items[*]}{@.metadata.name}:{range @.status.conditions[*]}{@.type}={@.status};{end}{end}'; until kubectl get pods -l app=etcd -o jsonpath="$JSONPATH" 2>&1 | grep -q "Ready=True"; do sleep 1; done

# Verify connectivity to the pachd pod
pachctl version
