build:
	cargo build
	docker build -t process -f ./docker_architecture/process/process.dockerfile .

run:
	mkdir -p experiment
	bash docker_architecture/experiment-setup.sh
	docker compose -f experiment/docker-compose.yml up --build

stop:
	docker compose -f experiment/docker-compose.yml down
	rm -rf experiment

docker-rm:
	docker volume rm $$(docker volume ls -q)

force-rm:
	(docker container prune -f; docker volume prune -f)
