## Development Environment

- Put the DAGs you want to run in the `dags` directory
- Run `docker-compose up` to start all the services
- If you want to add additional dependencies, add them to `setup.cfg` and re-run `docker-compose up`

## Useful Commands

- **Stop** all the services
    ```shell
    docker-compose down
    ```
- **Cleanup** the environment
    ```shell
    docker-compose down --volumes --remove-orphans
    ```
