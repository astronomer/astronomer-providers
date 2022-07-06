.PHONY: dev logs stop clean build build-emr_eks_container_example_dag-image build-aws build-google-cloud build-run docs
.PHONY: restart restart-all run-tests run-static-checks run-mypy run-local-lineage-server test-rc-deps help

# If the first argument is "run"...
ifeq (run-mypy,$(firstword $(MAKECMDGOALS)))
  # use the rest as arguments for "run"
  RUN_ARGS := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))
  ifndef RUN_ARGS
  RUN_ARGS := .
  endif
  # ...and turn them into do-nothing targets
  $(eval $(RUN_ARGS):;@:)
endif

dev: ## Create a development Environment using `docker-compose` file.
	docker-compose -f dev/docker-compose.yaml up -d

logs: ## View logs of the all the containers
	docker-compose -f dev/docker-compose.yaml logs --follow

stop: ## Stop all the containers
	docker-compose -f dev/docker-compose.yaml down

clean: ## Remove all the containers along with volumes
	docker-compose -f dev/docker-compose.yaml down  --volumes --remove-orphans
	rm -rf dev/logs

build: ## Build the Docker image (ignoring cache)
	docker build -f dev/Dockerfile . -t astronomer-providers-dev:latest --no-cache

build-emr_eks_container_example_dag-image: ## Build the Docker image for EMR EKS containers example DAG
	docker build -f dev/Dockerfile.emr_eks_container . -t astronomer-providers-dev:latest

build-aws: ## Build the Docker image with aws-cli installed
	docker build -f dev/Dockerfile.aws . -t astronomer-providers-dev:latest

build-google-cloud: ## Build the Docker image with google-cloud cli installed
	docker build -f dev/Dockerfile.google_cloud . -t astronomer-providers-dev:latest

build-run: ## Build the Docker Image & then run the containers
	docker-compose -f dev/docker-compose.yaml up --build -d

docs:  ## Build the docs using Sphinx
	cd docs && make clean html && cd .. && echo "Documentation built in $(shell pwd)/docs/_build/html/index.html"

restart: ## Restart Triggerer, Scheduler and Worker containers
	docker-compose -f dev/docker-compose.yaml restart airflow-triggerer airflow-scheduler airflow-worker

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

run-mypy: ## Run MyPy in Container
	docker build -f dev/Dockerfile . -t astronomer-providers-dev
	docker run -v `pwd`:/usr/local/airflow/astronomer_providers -v `pwd`/dev/.cache:/home/astro/.cache \
	 	-w /usr/local/airflow/astronomer_providers \
		--rm -it astronomer-providers-dev \
		-- mypy --install-types --cache-dir /home/astro/.cache/.mypy_cache $(RUN_ARGS)

run-local-lineage-server: ## Run flask based local Lineage server
	FLASK_APP=dev/local_flask_lineage_server.py flask run --host 0.0.0.0 --port 5050

test-rc-deps: ## Test providers RC by building an image with given dependencies and running the master DAG
	python3 dev/scripts/replace_dependencies.py '$(RC_PROVIDER_PACKAGES)'
	cd ".circleci/integration-tests/" && \
	 bash script.sh 'astro-cloud' '$(DOCKER_REGISTRY)' '$(ORGANIZATION_ID)' '$(DEPLOYMENT_ID)' '$(ASTRONOMER_KEY_ID)' '$(ASTRONOMER_KEY_SECRET)'
	python3 dev/scripts/trigger_master_dag.py '$(DEPLOYMENT_ID)' '$(ASTRONOMER_KEY_ID)' ' $(ASTRONOMER_KEY_SECRET)'
	git checkout setup.cfg

help: ## Prints this message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-41s\033[0m %s\n", $$1, $$2}'
