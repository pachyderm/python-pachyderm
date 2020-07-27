.PHONY: docker-build docker-push

docker-build:
	docker build -t pachyderm_spout_producer --no-cache .
	docker tag pachyderm_spout_producer ysimonson/pachyderm_spout_producer

docker-push: docker-build
	docker push ysimonson/pachyderm_spout_producer
