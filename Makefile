.PHONY: dev clean stop help

dev: ## Create a development Environment using `docker-compose` file.
	docker-compose -f dev/docker-compose.yaml up -d

logs: ## View logs of the all the containers
	docker-compose -f dev/docker-compose.yaml logs --follow

stop: ## Stop all the containers
	docker-compose -f dev/docker-compose.yaml down

clean: ## Remove all the containers along with volumes
	docker-compose -f dev/docker-compose.yaml down  --volumes --remove-orphans
	rm -rf dev/logs

help: ## Prints this message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
