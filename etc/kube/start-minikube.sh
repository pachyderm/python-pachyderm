#!/bin/bash

set -Eex

# Parse flags
KUBE_VERSION="$( jq -r .kubernetes <"$(git rev-parse --show-toplevel)/version.json" )"
while getopts ":v" opt; do
  case "${opt}" in
    v)
      KUBE_VERSION="v${OPTARG}"
      ;;
    \?)
      echo "Invalid argument: ${opt}"
      exit 1
      ;;
  esac
done
minikube version

minikube_args=(
  "--vm-driver=none"
  "--kubernetes-version=${KUBE_VERSION}"
)


# To make a 'sudo' run a binary in '~/cached-deps', you need to explicitly set
# the path like so:
#     sudo env "PATH=$PATH" minikube foo
sudo env "PATH=$PATH" "CHANGE_MINIKUBE_NONE_USER=true" \
  minikube start "${minikube_args[@]}"

# Try to connect for three minutes
for _ in $(seq 36); do
  if kubectl version &>/dev/null; then
    exit 0
  fi
  sleep 5
done

# Give up--kubernetes isn't coming up
minikube delete
sleep 30 # Wait for minikube to go completely down
exit 1
