.PHONY: docker-build docker-push

docker-build:
	docker build -t ysimonson/pachyderm_spout_producer --no-cache .

docker-push: docker-build
	docker push ysimonson/pachyderm_spout_producer
