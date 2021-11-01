SHELL := /bin/bash
PACHYDERM_VERSION ?= $(shell jq -r .pachyderm version.json)

docs:
	$(MAKE) -C docs html

docker-build-proto:
	cd proto && \
		docker build -t pachyderm_python_proto .

src/python_pachyderm/proto: docker-build-proto
	@echo "Building with pachyderm core v$(PACHYDERM_VERSION)"
	rm -rf src/python_pachyderm/proto
	cd proto/pachyderm && \
		git fetch --all && \
		git checkout v$(PACHYDERM_VERSION)
	find ./proto/pachyderm/src/client -regex ".*\.proto" \
	| grep -v 'internal' \
	| xargs tar cf - \
	| docker run -i pachyderm_python_proto \
	| tar xf -
	mv src/python_pachyderm/client src/python_pachyderm/proto
	find src/python_pachyderm/proto -type d -exec touch {}/__init__.py \;

init:
	git submodule update --init
	python -m pip install -U pip wheel setuptools
	python -m pip install -e .[DEV]
	pre-commit install

release:
	git checkout v6.x
	rm -rf build dist
	python3 setup.py sdist
	twine upload dist/*

test-release:
	git checkout v6.x
	rm -rf build dist
	sed -i "" 's/name="python-pachyderm"/name="python-pachyderm-test"/g' setup.py
	python3 setup.py sdist
	-twine upload --repository testpypi dist/*
	sed -i "" 's/name="python-pachyderm-test"/name="python-pachyderm"/g' setup.py

lint:
	black --check --diff .
	flake8 src/python_pachyderm etc setup.py
	PYTHONPATH=./src:$(PYTHONPATH) etc/proto_lint/proto_lint.py

.PHONY: docs docker-build-proto init release lint
