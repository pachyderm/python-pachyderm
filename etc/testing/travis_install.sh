#!/bin/bash

set -ex

# install latest version of docker
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
sudo apt-get update -y
sudo apt-get -y -o Dpkg::Options::="--force-confnew" install docker-ce

# reconfigure & restart docker
echo 'DOCKER_OPTS="-H unix:///var/run/docker.sock -s devicemapper"' | sudo tee /etc/default/docker > /dev/null
echo '{"experimental":true}' | sudo tee /etc/docker/daemon.json
sudo service docker restart

# Install deps
sudo apt-get install -y -qq \
  jq \
  silversearcher-ag \
  python3 \
  python3-pip \
  python3-setuptools \
  pkg-config \
  fuse \
  git \
  conntrack # for minikube

### Install kubectl
# Check cache
kubectl_valid="False"
target_kubectl="$( jq -r .kubernetes <"$(git rev-parse --show-toplevel)/version.json" )"
if [ ! -f ~/cached-deps/kubectl ]; then
  kubectl_version="$( ~/cached-deps/kubectl version --client=true | grep "GitVersion" | sed 's/^.*GitVersion:"v\([0-9.]\+\)".*$/\1/g' )"
  if [ "${kubectl_version}" == "${target_kubectl}" ]; then
    kubectl_valid="True"
  fi
fi

# To get the latest kubectl version:
# curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt
if [ "${kubectl_valid}" != "True" ]; then
    curl -L -o kubectl https://storage.googleapis.com/kubernetes-release/release/${target_kubectl}/bin/linux/amd64/kubectl && \
        chmod +x ./kubectl && \
        mv ./kubectl ~/cached-deps/kubectl
fi

### Install minikube
# Check cache
minikube_valid="False"
target_minikube="$( jq -r .minikube <"$(git rev-parse --show-toplevel)/version.json" )"
if [ ! -f ~/cached-deps/minikube ]; then
  minikube_version="$( ~/cached-deps/minikube version | grep 'minikube version:' | sed 's/^.*v\([0-9.]\+\)$/\1/g' )"
  if [ "${minikube_version}" == "${target_minikube}" ]; then
    minikube_valid="True"
  fi
fi

# To get the latest minikube version:
# curl https://api.github.com/repos/kubernetes/minikube/releases | jq -r .[].tag_name | sort -V | tail -n1
if [ "${minikube_valid}" != "True" ]; then
    curl -L -o minikube https://storage.googleapis.com/minikube/releases/${target_minikube}/minikube-linux-amd64 && \
        chmod +x ./minikube && \
        mv ./minikube ~/cached-deps/minikube
fi
