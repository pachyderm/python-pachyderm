#!/bin/bash

set -ex

mkdir -p cached-deps

# Install deps
sudo apt update -y
sudo apt-get install -y -qq \
  python3 \
  python3-pip \
  python3-setuptools \
  pkg-config \
  conntrack \
  pv \
  jq \
  socat

# Install kubectl
# To get the latest kubectl version:
# curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt
if [ ! -f cached-deps/kubectl ] ; then
    KUBECTL_VERSION=v1.19.2
    curl -L -o kubectl https://storage.googleapis.com/kubernetes-release/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl && \
        chmod +x ./kubectl
        mv ./kubectl cached-deps/kubectl
fi

# Install minikube
# To get the latest minikube version:
# curl https://api.github.com/repos/kubernetes/minikube/releases | jq -r .[].tag_name | sort -V | tail -n1
if [ ! -f cached-deps/minikube ] ; then
    MINIKUBE_VERSION=v1.19.0 # If changed, also do etc/kube/start-minikube.sh
    curl -L -o minikube https://storage.googleapis.com/minikube/releases/${MINIKUBE_VERSION}/minikube-linux-amd64 && \
        chmod +x ./minikube
        mv ./minikube cached-deps/minikube
fi

export PACHYDERM_VERSION="2.0.0-alpha.23"

# Install Pachyderm
curl -o /tmp/pachctl.deb -L https://github.com/pachyderm/pachyderm/releases/download/v${PACHYDERM_VERSION}/pachctl_${PACHYDERM_VERSION}_amd64.deb
sudo dpkg -i /tmp/pachctl.deb

# Install tox
pip install tox
