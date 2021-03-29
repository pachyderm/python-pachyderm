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
	find ./proto/pachyderm/ -regex ".*\.proto" \
	| grep -v 'internal' \
	| xargs tar cf - \
	| docker run -i pachyderm_python_proto \
	| tar xf -
	rm -rf src/python_pachyderm/proto
	mv src/python_pachyderm/src src/python_pachyderm/proto
	find src/python_pachyderm/proto -type d -exec touch {}/__init__.py \;

init:
	git submodule update --init

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
	until timeout 1s ./etc/kube/check_ready.sh app=pachd; do sleep 1; done
	pachctl version

release:
	git checkout v7.x
	rm -rf build dist
	python3 setup.py sdist
	twine upload dist/*

lint:
	flake8 src/python_pachyderm --exclude=src/python_pachyderm/proto --max-line-length=120 --max-doc-length=80
	PYTHONPATH=./src:$(PYTHONPATH) etc/proto_lint/proto_lint.py

.PHONY: docs docker-build-proto init ci-install ci-setup release lint
