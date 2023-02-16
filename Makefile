SHELL := /bin/bash
PACHYDERM_VERSION ?= $(shell jq -r .pachyderm version.json)

docs:
	$(MAKE) -C docs html

docker-build-proto:
	docker build -t pachyderm_python_proto proto

src/python_pachyderm/proto/v2: docker-build-proto
	@echo "Building with pachyderm core v$(PACHYDERM_VERSION)"
	rm -rf src/python_pachyderm/proto/v2 && \
		rm -rf src/python_pachyderm/experimental/proto/v2
	cd proto/pachyderm && \
		git fetch --all && \
		git checkout v$(PACHYDERM_VERSION)
	find ./proto/pachyderm/src -regex ".*\.proto" \
	| grep -v 'internal' \
	| grep -v 'server' \
	| grep -v 'protoextensions' \
	| xargs tar cf - \
	| docker run -e VERSION=2 -i pachyderm_python_proto \
	| tar -C src -xf -

init:
	git submodule update --init
	pre-commit install

release:
	git checkout origin/master
	rm -rf build dist
	poetry publish --build

test-release:
	git checkout origin/master
	rm -rf build dist
	poetry publish --build --repository testpypi

lint:
	black --check --diff .
	flake8 .
	PYTHONPATH=./src:$(PYTHONPATH) etc/proto_lint/proto_lint.py

.PHONY: docs docker-build-proto init release test-release lint
