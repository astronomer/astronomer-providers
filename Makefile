.PHONY: dev logs stop clean build build-emr_eks_container_example_dag-image build-aws build-google-cloud build-run docs
.PHONY: restart restart-all run-tests run-static-checks run-mypy run-local-lineage-server test-rc-deps shell help

ASTRO_PROVIDER_VERSION ?= "dev"

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

ASTRO_RUNTIME_IMAGE_NAME = "quay.io/astronomer/astro-runtime:11.9.0-slim-base"

dev: ## Create a development Environment using `docker compose` file.
	IMAGE_NAME=$(ASTRO_RUNTIME_IMAGE_NAME) docker compose -f dev/docker-compose.yaml up -d

logs: ## View logs of the all the containers
	docker compose -f dev/docker-compose.yaml logs --follow

stop: ## Stop all the containers
	docker compose -f dev/docker-compose.yaml down

clean: ## Remove all the containers along with volumes
	docker compose -f dev/docker-compose.yaml down  --volumes --remove-orphans
	rm -rf dev/logs

build: ## Build the Docker image (ignoring cache)
	docker build --build-arg IMAGE_NAME=$(ASTRO_RUNTIME_IMAGE_NAME) -f dev/Dockerfile . -t astronomer-providers-dev:latest --no-cache

build-emr_eks_container_example_dag-image: ## Build the Docker image for EMR EKS containers example DAG
	docker build --build-arg IMAGE_NAME=$(ASTRO_RUNTIME_IMAGE_NAME) -f dev/Dockerfile.emr_eks_container . -t astronomer-providers-dev:latest

build-aws: ## Build the Docker image with aws-cli installed
	docker build --build-arg IMAGE_NAME=$(ASTRO_RUNTIME_IMAGE_NAME) -f dev/Dockerfile.aws . -t astronomer-providers-dev:latest

build-google-cloud: ## Build the Docker image with google-cloud cli installed
	docker build --build-arg IMAGE_NAME=$(ASTRO_RUNTIME_IMAGE_NAME) -f dev/Dockerfile.google_cloud . -t astronomer-providers-dev:latest

build-run: ## Build the Docker Image & then run the containers
	IMAGE_NAME=$(ASTRO_RUNTIME_IMAGE_NAME) docker compose -f dev/docker-compose.yaml up --build -d

docs:  ## Build the docs using Sphinx
	cd docs && make clean html && cd .. && echo "Documentation built in $(shell pwd)/docs/_build/html/index.html"

restart: ## Restart Triggerer, Scheduler and Worker containers
	docker compose -f dev/docker-compose.yaml restart airflow-triggerer airflow-scheduler airflow-worker

restart-all: ## Restart all the containers
	docker compose -f dev/docker-compose.yaml restart

run-tests: ## Run CI tests
	docker build --build-arg IMAGE_NAME=$(ASTRO_RUNTIME_IMAGE_NAME) -f dev/Dockerfile . -t astronomer-providers-dev
	docker run -v `pwd`:/usr/local/airflow/astronomer_providers -v `pwd`/dev/.cache:/home/astro/.cache \
	 	-w /usr/local/airflow/astronomer_providers \
		--rm -it astronomer-providers-dev -- pytest --cov astronomer --cov-report=term-missing tests

run-static-checks: ## Run CI static code checks
	docker build --build-arg IMAGE_NAME=$(ASTRO_RUNTIME_IMAGE_NAME) -f dev/Dockerfile . -t astronomer-providers-dev
	docker run -v `pwd`:/usr/local/airflow/astronomer_providers -v `pwd`/dev/.cache:/home/astro/.cache \
	 	-w /usr/local/airflow/astronomer_providers \
		--rm -it astronomer-providers-dev -- pre-commit run --all-files --show-diff-on-failure

run-mypy: ## Run MyPy in Container
	docker build --build-arg IMAGE_NAME=$(ASTRO_RUNTIME_IMAGE_NAME) -f dev/Dockerfile . -t astronomer-providers-dev
	docker run -v `pwd`:/usr/local/airflow/astronomer_providers -v `pwd`/dev/.cache:/home/astro/.cache \
	 	-w /usr/local/airflow/astronomer_providers \
		--rm -it astronomer-providers-dev \
		-- mypy --install-types --cache-dir /home/astro/.cache/.mypy_cache $(RUN_ARGS)

run-local-lineage-server: ## Run flask based local Lineage server
	FLASK_APP=dev/local_flask_lineage_server.py flask run --host 0.0.0.0 --port 5050

test-rc-deps: ## Test providers RC by building an image with given dependencies and running the master DAG
	@which gh > /dev/null || (echo "ERROR: Github CLI is required. Refer https://github.com/cli/cli for installation."; exit 1)
	python3 -m pip install -r dev/integration_test_scripts/requirements.txt
	git checkout main && git pull origin main
	$(eval current_timestamp := $(shell date +%Y-%m-%dT%H-%M-%S%Z))
	echo "Current timestamp is" $(current_timestamp)
	$(eval branch_name := "rc-test-$(current_timestamp)")
	git checkout -b $(branch_name)
	echo "Updating setup.cfg with RC provider packages"
	python3 dev/integration_test_scripts/replace_dependencies.py --issue-url '$(ISSUE_URL)'
	echo "Building and deploying image to Astro Cloud"
	cd ".circleci/integration-tests/" && \
	 bash script.sh 'astro-cloud' '$(ASTRO_SUBDOMAIN)' '$(DOCKER_REGISTRY)' '$(ORGANIZATION_ID)' '$(DEPLOYMENT_ID)' '$(ASTRONOMER_KEY_ID)' '$(ASTRONOMER_KEY_SECRET)'
	$(eval current_timestamp := $(shell date))
	echo "Current timestamp is" $(current_timestamp)
	echo "Sleeping for 1800 seconds (30 minutes) allowing the deployed image to be updated across all Airflow components.."
	sleep 1800
	python3 dev/integration_test_scripts/trigger_dag.py '$(DEPLOYMENT_ID)' '$(ASTRONOMER_KEY_ID)' ' $(ASTRONOMER_KEY_SECRET)'
	git add setup.cfg
	git commit -m "Update setup.cfg to use RC provider packages"
	git push origin $(branch_name)
	gh pr create --base "main" --title "[DO NOT MERGE] Test RC provider packages" --fill

shell:  ## Runs a shell within a container (Allows interactive session)
	docker compose -f dev/docker-compose.yaml run --rm airflow-scheduler bash

bump-version:  ## Bump versions in files locally. By default bump to DEV. set env ASTRO_PROVIDER_VERSION to change default version. see docs https://github.com/astronomer/astronomer-providers/blob/main/RELEASING.rst#update-the-version-number
	@if [ $(ASTRO_PROVIDER_VERSION) = "dev" ]; then\
		cz bump --version-type semver --increment minor --devrelease 1 --files-only;\
	else\
		cz bump $(ASTRO_PROVIDER_VERSION) --files-only;\
    fi

prepare-release:  ## Create a release branch, bump version and draft changelog. set env ASTRO_PROVIDER_VERSION to change default version. see docs https://github.com/astronomer/astronomer-providers/blob/main/RELEASING.rst#update-the-version-number
	$(eval branch_name := "release-$(subst .,-,$(ASTRO_PROVIDER_VERSION))")
	git checkout -b $(branch_name)
	cz bump $(ASTRO_PROVIDER_VERSION) --files-only
	python dev/prepare_release_scripts/changelog_drafter.py $(ASTRO_PROVIDER_VERSION)
	echo "We still need to update the CHANGELOG.rst manually."

help: ## Prints this message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-41s\033[0m %s\n", $$1, $$2}'
