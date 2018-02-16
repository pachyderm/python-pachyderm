SHELL := /bin/bash

docker-build-proto:
	pushd proto && \
		docker build -t pachyderm_python_proto .

proto: docker-build-proto
	find ./proto/pachyderm/src/client -maxdepth 2 -regex ".*\.proto" \
	| xargs tar cf - \
	| docker run -i pachyderm_python_proto \
	| tar xf -

test:
	pip uninstall protobuf || true
	pip uninstall google || true
	pip install google
	pip install protobuf
	pip install -r proto/requirements.txt
	# Need to temporarily remove the pachyderm code base, otherwise pytest
	# complains about python files in there
	mv proto/pachyderm proto/.pachyderm || true
	# This is hacky, but the alternative seems to be hacking a grpc generated file,
	# which is a no-no
	echo "$$PYTHONPATH:$$PWD/src:$$PWD/src/python_pachyderm:$$PWD/src/python_pachyderm/proto"
	echo "list of installed stuff:"
	pip3 freeze
	PYTHONPATH="$$PYTHONPATH:$$PWD/src:$$PWD/src/python_pachyderm:$$PWD/src/python_pachyderm/proto" pytest
	mv proto/.pachyderm proto/pachyderm

init:
	git submodule update --init

ci-setup:
	pushd proto/pachyderm && \
		sudo ./etc/testing/ci/before_install.sh && \
		curl -o /tmp/pachctl.deb -L https://github.com/pachyderm/pachyderm/releases/download/v$$(cat ../../VERSION)/pachctl_$$(cat ../../VERSION)_amd64.deb  && \
		sudo dpkg -i /tmp/pachctl.deb && \
		make launch-kube && \
		docker version && \
		echo "pachctl location:" && \
		which pachctl && \
		pachctl deploy local --dry-run && \
		( pachctl deploy local --dry-run | kubectl create -f - ) && \
		until timeout 1s ./etc/kube/check_ready.sh app=pachd; do sleep 1; done && \
	popd

sync:
	# NOTE: This task must be run like:
	# PACHYDERM_VERSION=1.2.3 make sync
	if [[ -z "$$PACHYDERM_VERSION" ]]; then \
		exit 1; \
	fi
	echo $$PACHYDERM_VERSION > VERSION
	# Will update the protos to match the VERSION file
	pushd proto/pachyderm && \
		git fetch --all && \
		git checkout $$(cat ../../VERSION) && \
		popd
	# Rebuild w latest proto files
	make proto

release:
	python setup.py register -r pypi
	python setup.py sdist upload -r pypi


.PHONY: test
