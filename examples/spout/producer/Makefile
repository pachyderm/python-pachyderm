.PHONY: docker-build

docker-build:
	docker build -t pachyderm_example_spout_producer --no-cache .
	docker tag pachyderm_example_spout_producer ysimonson/pachyderm_example_spout_producer
	docker push ysimonson/pachyderm_example_spout_producer
