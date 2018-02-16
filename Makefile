SHELL := /bin/bash

docker-build-proto:
	pushd proto && \
		docker build -t pachyderm_python_proto .

proto: docker-build-proto init
	find ./proto/pachyderm/src/client -maxdepth 2 -regex ".*\.proto" \
	| xargs tar cf - \
	| docker run -i pachyderm_python_proto \
	| tar xf -

test:
	# Need to temporarily remove the pachyderm code base, otherwise pytest
	# complains about python files in there
	rm -rf proto/pachyderm || true
	# This is hacky, but the alternative seems to be hacking a grpc generated file,
	# which is a no-no
	PYTHONPATH="$$PYTHONPATH:$$PWD/src:$$PWD/src/python_pachyderm:$$PWD/src/python_pachyderm/proto" pytest
	make init

init:
	git submodule update --init

ci-setup:
	pushd proto/pachyderm && \
		sudo ./etc/testing/ci/before_install.sh && \
		curl -o /tmp/pachctl.deb -L https://github.com/pachyderm/pachyderm/releases/download/$$(cat VERSION)/pachctl_$$(cat VERSION)_amd64.deb  && \
		sudo dpkg -i /tmp/pachctl.deb && \
		make launch-kube && \
		docker version && \
		make launch-dev && \
	popd

.PHONY: test
