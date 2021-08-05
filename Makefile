SHELL := /bin/bash
PACHYDERM_VERSION ?= $(shell jq -r .pachyderm version.json)

docs:
	rm -rf docs
	pdoc --html -o docs python_pachyderm

docker-build-proto:
	docker build -t pachyderm_python_proto proto

src/python_pachyderm/proto/v2: docker-build-proto
	@echo "Building with pachyderm core v$(PACHYDERM_VERSION)"
	rm -rf src/python_pachyderm/proto/v2
	cd proto/pachyderm && \
		git fetch --all && \
		git checkout v$(PACHYDERM_VERSION)
	find ./proto/pachyderm/src -regex ".*\.proto" \
	| grep -v 'internal' \
	| grep -v 'server' \
	| xargs tar cf - \
	| docker run -e VERSION=2 -i pachyderm_python_proto \
	| tar -C src -xf -

init:
	git submodule update --init
	python -m pip install -U pip wheel setuptools
	python -m pip install -e ".[DEV]"
	pre-commit install

ci-install:
	sudo apt-get update
	sudo apt-get install jq socat
	sudo etc/testing/travis_cache.sh
	sudo etc/testing/travis_install.sh
	curl -o /tmp/pachctl.deb -L https://github.com/pachyderm/pachyderm/releases/download/v$(PACHYDERM_VERSION)/pachctl_$(PACHYDERM_VERSION)_amd64.deb
	sudo dpkg -i /tmp/pachctl.deb
	pip install tox tox-travis

ci-setup:
	docker version
	which pachctl
	etc/kube/start-minikube.sh
	echo 'y' | pachctl deploy local
	sleep 20
	kubectl wait --for=condition=ready pod -l app=pachd --timeout=5m
	pachctl version

release:
	git checkout master
	rm -rf build dist
	python3 setup.py sdist
	twine upload dist/*

test-release:
	git checkout master
	rm -rf build dist
	sed -i "" 's/name="python-pachyderm"/name="python-pachyderm-test"/g' setup.py
	python3 setup.py sdist
	-twine upload --repository testpypi dist/*
	sed -i "" 's/name="python-pachyderm-test"/name="python-pachyderm"/g' setup.py

lint:
	black --check --diff .
	flake8 .
	PYTHONPATH=./src:$(PYTHONPATH) etc/proto_lint/proto_lint.py

.PHONY: docs docker-build-proto init ci-install ci-setup release test-release lint
