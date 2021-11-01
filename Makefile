SHELL := /bin/bash
PACHYDERM_VERSION ?= $(shell jq -r .pachyderm version.json)

docs:
	$(MAKE) -C docs html

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

release:
	git checkout <branch>
	rm -rf build dist
	python3 setup.py sdist
	twine upload dist/*

test-release:
	git checkout <branch>
	rm -rf build dist
	sed -i "" 's/name="python-pachyderm"/name="python-pachyderm-test"/g' setup.py
	python3 setup.py sdist
	-twine upload --repository testpypi dist/*
	sed -i "" 's/name="python-pachyderm-test"/name="python-pachyderm"/g' setup.py

lint:
	black --check --diff .
	flake8 .
	PYTHONPATH=./src:$(PYTHONPATH) etc/proto_lint/proto_lint.py

.PHONY: docs docker-build-proto init release test-release lint
