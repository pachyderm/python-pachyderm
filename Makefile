SHELL := /bin/bash

docs:
	python3 setup.py clean build install
	pdoc3 --html --html-dir ./docs ./src/python_pachyderm

docker-build-proto:
	cd proto && \
		docker build -t pachyderm_python_proto .

proto: docker-build-proto
	find ./proto/pachyderm/src/client -regex ".*\.proto" \
	| xargs tar cf - \
	| docker run -i pachyderm_python_proto \
	| tar xf -

init:
	git submodule update --init

ci-install:
	sudo apt-get install jq
	cd proto/pachyderm && \
		sudo ./etc/testing/travis_before_install.sh && \
		curl -o /tmp/pachctl.deb -L https://github.com/pachyderm/pachyderm/releases/download/v$$(jq -r .pachyderm version.json)/pachctl_$$(jq -r .pachyderm version.json)_amd64.deb  && \
		sudo dpkg -i /tmp/pachctl.deb
	pip install tox tox-travis

ci-setup:
	docker version
	which pachctl
	cd proto/pachyderm && make launch-kube
	pachctl deploy local
	until timeout 1s ./proto/pachyderm/etc/kube/check_ready.sh app=pachd; do sleep 1; done
	PACHD_ADDRESS=$$(minikube ip):30650 pachctl version

sync:
	cd proto/pachyderm && \
		git fetch --all && \
		git checkout v$$(jq -r .pachyderm version.json)
	make proto

release:
	rm -rf build dist
	python setup.py sdist
	twine upload dist/*

.PHONY: docker-build-proto init ci-install ci-setup sync release
