SHELL := /bin/bash
PACHYDERM_VERSION ?= $(shell jq -r .pachyderm version.json)

docs:
	rm -rf build dist
	python3 setup.py clean build install
	pdoc3 --html --html-dir ./docs ./src/python_pachyderm

docker-build-proto:
	cd proto && \
		docker build -t pachyderm_python_proto .

src/python_pachyderm/proto: docker-build-proto
	@echo "Building with pachyderm core v$(PACHYDERM_VERSION)"
	cd proto/pachyderm && \
		git fetch --all && \
		git checkout v$(PACHYDERM_VERSION)
	find ./proto/pachyderm/src/client -regex ".*\.proto" \
	| xargs tar cf - \
	| docker run -i pachyderm_python_proto \
	| tar xf -
	mv src/python_pachyderm/client src/python_pachyderm/proto
	find src/python_pachyderm/proto -type d -exec touch {}/__init__.py \;

init:
	git submodule update --init

ci-install:
	sudo apt-get install jq
	cd proto/pachyderm && \
		sudo ./etc/testing/travis_before_install.sh && \
		curl -o /tmp/pachctl.deb -L https://github.com/pachyderm/pachyderm/releases/download/v$(PACHYDERM_VERSION)/pachctl_$(PACHYDERM_VERSION)_amd64.deb  && \
		sudo dpkg -i /tmp/pachctl.deb
	pip install tox tox-travis

ci-setup:
	docker version
	which pachctl
	cd proto/pachyderm && make launch-kube
	pachctl deploy local
	until timeout 1s ./proto/pachyderm/etc/kube/check_ready.sh app=pachd; do sleep 1; done
	PACHD_ADDRESS=$$(minikube ip):30650 pachctl version

release:
	rm -rf build dist
	python setup.py sdist
	twine upload dist/*

.PHONY: docker-build-proto init ci-install ci-setup release
