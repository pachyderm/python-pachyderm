.PHONY: docker-build

docker-build:
	docker build -t pachyderm_spout_producer .
	docker tag pachyderm_spout_producer ysimonson/pachyderm_spout_producer
	docker push ysimonson/pachyderm_spout_producer
