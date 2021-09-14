SHELL := /bin/bash
PACHYDERM_VERSION ?= $(shell jq -r .pachyderm version.json)

docs:
	# NOTE: do not run this in a virtualenv -- instead, ensure that
	# python-pachyderm is installed to the "system" python3 site-packages.
	# This is for a couple of reasons:
	# 1) pdoc has totally different flows for virtualenv-based vs system-based
	# packages, and will generate different docs.
	# 2) pdoc will frequently ignore virtualenv anyways.
	sudo rm -rf build dist
	python3 setup.py clean build
	sudo python3 setup.py install
	pdoc --html --html-dir docs python_pachyderm

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

lint:
	black --check --diff .
	flake8 src/python_pachyderm etc setup.py
	PYTHONPATH=./src:$(PYTHONPATH) etc/proto_lint/proto_lint.py

.PHONY: docs docker-build-proto init release lint
