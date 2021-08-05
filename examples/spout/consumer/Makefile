.PHONY: docker-build docker-push

docker-build:
	docker build -t pachyderm/example-python-spout-consumer:7.0.0a6 .

docker-push: docker-build
	docker push pachyderm/example-python-spout-consumer:7.0.0a6
