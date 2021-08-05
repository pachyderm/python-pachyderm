.PHONY: docker-build docker-push

docker-build:
	docker build -t pachyderm/example-python-spout-consumer .

docker-push: docker-build
	docker push pachyderm/example-python-spout-consumer
