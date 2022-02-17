.PHONY: dev clean stop build-run restart restart-all run-tests run-static-checks help

dev: ## Create a development Environment using `docker-compose` file.
	docker-compose -f dev/docker-compose.yaml up -d

logs: ## View logs of the all the containers
	docker-compose -f dev/docker-compose.yaml logs --follow

stop: ## Stop all the containers
	docker-compose -f dev/docker-compose.yaml down

clean: ## Remove all the containers along with volumes
	docker-compose -f dev/docker-compose.yaml down  --volumes --remove-orphans
	rm -rf dev/logs

build-run: ## Build the Docker Image & then run the containers
	docker-compose -f dev/docker-compose.yaml up --build -d

restart: ## Restart Triggerer & Scheduler container
	docker-compose -f dev/docker-compose.yaml restart airflow-triggerer airflow-scheduler

restart-all: ## Restart all the containers
	docker-compose -f dev/docker-compose.yaml restart

run-tests: ## Run CI tests
	docker build -f .circleci/scripts/Dockerfile . -t astronomer-operators-ci
	docker run --rm -it astronomer-operators-ci pytest tests

run-static-checks: ## Run CI static code checks
	docker build -f .circleci/scripts/Dockerfile . -t astronomer-operators-ci
	docker run --rm -it astronomer-operators-ci pre-commit run --all-files

help: ## Prints this message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
