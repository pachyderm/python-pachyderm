.PHONY: docker-build

docker-build:
	docker build -t pachyderm_spout_printer .
	docker tag pachyderm_spout_printer ysimonson/pachyderm_spout_printer
	docker push ysimonson/pachyderm_spout_printer
