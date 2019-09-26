SHELL := /bin/bash

docs:
	python3 setup.py clean build install
	pdoc3 --html --html-dir ./docs ./src/python_pachyderm

docker-build-proto:
	pushd proto && \
		docker build -t pachyderm_python_proto .

proto: docker-build-proto
	find ./proto/pachyderm/src/client -regex ".*\.proto" \
	| xargs tar cf - \
	| docker run -i pachyderm_python_proto \
	| tar xf -
	rm -rf src/python_pachyderm/proto
	mv src/python_pachyderm/client src/python_pachyderm/proto
	find src/python_pachyderm/proto -type d -exec touch {}/__init__.py \;

init:
	git submodule update --init

ci-install:
	pushd proto/pachyderm && \
		sudo ./etc/testing/travis_before_install.sh && \
		curl -o /tmp/pachctl.deb -L https://github.com/pachyderm/pachyderm/releases/download/v$$(cat ../../VERSION)/pachctl_$$(cat ../../VERSION)_amd64.deb  && \
		sudo dpkg -i /tmp/pachctl.deb && \
		popd
	pip install tox tox-travis

ci-setup:
	docker version
	which pachctl
	pushd proto/pachyderm && \
		make launch-kube && \
		popd
	pachctl deploy local
	until timeout 1s ./proto/pachyderm/etc/kube/check_ready.sh app=pachd; do sleep 1; done
	PACHD_ADDRESS=$$(minikube ip):30650 pachctl version

sync:
	# NOTE: This task must be run like:
	# PACHYDERM_VERSION=1.2.3 make sync
	if [[ -z "$$PACHYDERM_VERSION" ]]; then \
		exit 1; \
	fi
	echo $$PACHYDERM_VERSION > VERSION
	echo 0 > BUILD_NUMBER
	# Will update the protos to match the VERSION file
	pushd proto/pachyderm && \
		git fetch --all && \
		git checkout v$$(cat ../../VERSION) && \
		popd
	# Rebuild w latest proto files
	make proto

release:
	rm -rf build dist
	# Bump the version
	expr $$(cat BUILD_NUMBER) + 1 > BUILD_NUMBER
	python setup.py sdist
	twine upload dist/*

.PHONY: docker-build-proto init ci-install ci-setup sync release
