.PHONY: docker-build

docker-build:
	docker build -t pachyderm_spout_producer --no-cache .
	docker tag pachyderm_spout_producer ysimonson/pachyderm_spout_producer
	docker push ysimonson/pachyderm_spout_producer
