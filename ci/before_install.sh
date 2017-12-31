#!/bin/bash

# -e : Readline  is used to obtain the line
# -x : Print a trace of commands and their arguments after they are expanded and before they are executed
set -ex

# Download kubectl and install kubectl
curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/v1.9.0/bin/linux/amd64/kubectl \
    && chmod +x kubectl \
    && sudo mv kubectl /usr/local/bin/

# Download and install minikube
curl -Lo minikube https://storage.googleapis.com/minikube/releases/v0.24.1/minikube-linux-amd64 \
    && chmod +x minikube \
    && sudo mv minikube /usr/local/bin/

# Download and install pachctl
curl -o /tmp/pachctl.deb -L https://github.com/pachyderm/pachyderm/releases/download/v1.6.6/pachctl_1.6.6_amd64.deb \
    && sudo dpkg -i /tmp/pachctl.deb
