SHELL := /bin/bash

docker-build-proto:
	pushd proto && \
		docker build -t pachyderm_python_proto .

proto: docker-build-proto
	find ./proto -regex ".*\.proto" \
	| xargs tar cf - \
	| docker run -i pachyderm_python_proto \
	| tar xf -

release:
	@#Checkout version of pachyderm as specified by version file
	pushd proto/pachyderm && \
		checkout $$(cat VERSION) && \
		popd
	make build-proto
	echo "Updated client to use pachyderm lib @ $$(cat VERSION)"
	echo "Please commit the changes"

test:
	@#PYTHONPATH="$$PYTHONPATH:$$PWD:$$PWD/python_pachyderm:$$PWD/python_pachyderm/google" ./tests/test_pfs_client.py
	# This is hacky, but the alternative seems to be hacking a grpc generated file,
	# which is a no-no
	PYTHONPATH="$$PYTHONPATH:$$PWD/src/python_pachyderm" pytest
