SHELL := /bin/bash

docker-build-proto:
	pushd proto && \
		docker build -t pachyderm_python_proto .

proto: docker-build-proto
	find ./proto/pachyderm/src/client -maxdepth 2 -regex ".*\.proto" \
	| xargs tar cf - \
	| docker run -i pachyderm_python_proto \
	| tar xf -

init:
	git submodule update --init

ci-setup:
	pushd proto/pachyderm && \
		sudo ./etc/testing/ci/before_install.sh && \
		curl -o /tmp/pachctl.deb -L https://github.com/pachyderm/pachyderm/releases/download/v$$(cat ../../VERSION)/pachctl_$$(cat ../../VERSION)_amd64.deb  && \
		sudo dpkg -i /tmp/pachctl.deb && \
		make launch-kube && \
		popd
	docker version
	which pachctl
	pachctl deploy local
	until timeout 1s ./proto/pachyderm/etc/kube/check_ready.sh app=pachd; do sleep 1; done
	pachctl version

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
		git checkout v$$(cat ../../VERSION) && \
		popd
	# Rebuild w latest proto files
	make proto

release:
	# Bump the version
	expr $$(cat BUILD_NUMBER_NUMBER) + 1 > BUILD_NUMBER_NUMBER
	python setup.py sdist
	twine upload dist/*

.PHONY: test
