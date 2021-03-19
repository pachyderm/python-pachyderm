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
	cd proto/pachyderm && \
		git fetch --all && \
		git checkout v$(PACHYDERM_VERSION)
	find ./proto/pachyderm/src -regex ".*\.proto" \
	| grep -v 'internal' \
	| xargs tar cf - \
	| docker run -i pachyderm_python_proto \
	| tar xf -
	find src/python_pachyderm/proto -type d -exec touch {}/__init__.py \;

init:
	git submodule update --init
	python -m pip install -U pip wheel setuptools
	python -m pip install -e .[DEV]
	pre-commit install

ci-install:
	sudo apt-get update
	sudo apt-get install jq socat
	cd proto/pachyderm && \
		sudo etc/testing/travis_cache.sh && \
		sudo etc/testing/travis_install.sh && \
		curl -o /tmp/pachctl.deb -L https://github.com/pachyderm/pachyderm/releases/download/v$(PACHYDERM_VERSION)/pachctl_$(PACHYDERM_VERSION)_amd64.deb  && \
		sudo dpkg -i /tmp/pachctl.deb
	pip install tox tox-travis

ci-setup:
	docker version
	which pachctl
	cd proto/pachyderm && make launch-kube
	pachctl deploy local
	until timeout 1s ./proto/pachyderm/etc/kube/check_ready.sh app=pachd; do sleep 1; done
	pachctl version

release:
	git checkout master
	rm -rf build dist
	python3 setup.py sdist
	twine upload dist/*

lint:
	black --check --diff .
	flake8 src/python_pachyderm etc setup.py
	PYTHONPATH=./src:$(PYTHONPATH) etc/proto_lint/proto_lint.py

.PHONY: docker-build-proto init ci-install ci-setup release lint
