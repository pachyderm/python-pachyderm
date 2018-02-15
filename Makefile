SHELL := /bin/bash

docker-build-proto:
	pushd proto && \
		docker build -t pachyderm_python_proto .

proto: docker-build-proto
	find ./proto -regex ".*\.proto" \
	| xargs tar cf - \
	| docker run -i pachyderm_python_proto \
	| tar xf -

test:
	@#PYTHONPATH="$$PYTHONPATH:$$PWD:$$PWD/python_pachyderm:$$PWD/python_pachyderm/google" ./tests/test_pfs_client.py
	# This is hacky, but the alternative seems to be hacking a grpc generated file,
	# which is a no-no
	PYTHONPATH="$$PYTHONPATH:$$PWD/src:$$PWD/src/python_pachyderm" pytest

ci-setup:
	sudo etc/pachyderm/etc/testing/ci/before_install.sh
	curl -o /tmp/pachctl.deb -L https://github.com/pachyderm/pachyderm/releases/download/$$(cat VERSION)/pachctl_$$(cat VERSION)_amd64.deb \
	  && sudo dpkg -i /tmp/pachctl.deb
	which pachctl
	pushd etc/pachyderm && \
		make launch-kube && \
		docker version && \
		make launch-dev && \
	popd

ci-ssh:
	ssh-keygen -t rsa -b 4096 -C "buildbot@pachyderm.io" -f $${HOME}/.ssh/id_rsa -N ''
	echo "generated ssh keys:"
	ls ~/.ssh
	cat ~/.ssh/id_rsa.pub

