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
	docker build -f dev/Dockerfile . -t astronomer-providers-dev
	docker run -v `pwd`:/usr/local/airflow/astronomer_providers -v `pwd`/dev/.cache:/home/astro/.cache \
	 	-w /usr/local/airflow/astronomer_providers \
		--rm -it astronomer-providers-dev -- pytest tests

run-static-checks: ## Run CI static code checks
	docker build -f dev/Dockerfile . -t astronomer-providers-dev
	docker run -v `pwd`:/usr/local/airflow/astronomer_providers -v `pwd`/dev/.cache:/home/astro/.cache \
	 	-w /usr/local/airflow/astronomer_providers \
		--rm -it astronomer-providers-dev -- pre-commit run --all-files --show-diff-on-failure

help: ## Prints this message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
