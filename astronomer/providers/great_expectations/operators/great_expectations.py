import os
from datetime import datetime
from typing import Any, Callable, Dict, Optional, Union

import great_expectations as ge
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from great_expectations.checkpoint import Checkpoint
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    BaseStoreBackendDefaults,
    CheckpointConfig,
    DataContextConfig,
    FilesystemStoreBackendDefaults,
)
from great_expectations.data_context.util import instantiate_class_from_config


class GreatExpectationsOperator(BaseOperator):
    """
    An operator to leverage Great Expectations as a task in your Airflow DAG.

    Current list of expectations types:
    https://docs.greatexpectations.io/en/latest/reference/glossary_of_expectations.html

    How to create expectations files:
    https://docs.greatexpectations.io/en/latest/guides/tutorials/how_to_create_expectations.html

    :param run_name: Identifies the validation run (defaults to timestamp if not specified)
    :type run_name: Optional[str]
    :param conn_id: The name of a connection in Airflow
    :type conn_id: Optional[str]
    :param base_directory: The directory an InferredAssetFilesystemDataConnector will search for files under
    :type base_directory: Optional[str]
    :param file_regex: The regex used to load files with an InferredAssetFilesystemDataConnector
    :type file_regex: Optional[Dict]
    :param execution_engine: The execution engine to use when running Great Expectations
    :type execution_engine: Optional[str]
    :param store_backend_defaults: Determines how backend defaults are stored
    :type store_backend_defaults: Optional[BaseStoreBackendDefaults]
    :param batch_kwargs: Key-word arguments for a batch request
    :type batch_kwargs: Optional[Dict]
    :param expectation_suite_name: Name of the expectation suite to run if using a default Data Context and default
        Checkpoint
    :type expectation_suite_name: Optional[str]
    :param data_connector: The type of data connector to use
    :type data_connector: Optional[Dict]
    :param data_asset_name: The name of the table or dataframe that the default Data Context will load and default
        Checkpoint will run over
    :type data_asset_name: Optional[str]
    :param data_context_root_dir: Path of the great_expectations directory
    :type data_context_root_dir: Optional[str]
    :param data_context_config: A great_expectations `DataContextConfig` object
    :type data_context_config: Optional[DataContextConfig]
    :param checkpoint_name: A Checkpoint name to use for validation
    :type checkpoint_name: Optional[str]
    :param checkpoint_config: A great_expectations `CheckpointConfig` object to use for validation
    :type checkpoint_config: Optional[CheckpointConfig]
    :param checkpoint_kwargs: A dictionary whose keys match the parameters of CheckpointConfig which can be used to
        update and populate the Operator's Checkpoint at runtime
    :type checkpoint_kwargs: Optional[Dict]
    :param fail_task_on_validation_failure: Fail the Airflow task if the Great Expectation validation fails
    :type fail_task_on_validation_failure: bool
    :param validation_failure_callback: Called when the Great Expectations validation fails
    :type validation_failure_callback: Callable[[CheckpointResult], None]
    :param return_json_dict: If True, returns a json-serializable dictionary instead of a CheckpointResult object
    :type return_json_dict: bool
    """

    ui_color = "#AFEEEE"
    ui_fgcolor = "#000000"

    def __init__(
        self,
        run_name: Optional[str] = None,
        conn_id: Optional[str] = None,
        base_directory: Optional[str] = None,
        file_regex: Optional[Dict] = None,
        execution_engine: Optional[str] = None,
        store_backend_defaults: Optional[BaseStoreBackendDefaults] = None,
        batch_kwargs: Optional[Dict] = None,
        expectation_suite_name: Optional[str] = None,
        data_connector: Optional[Dict] = None,
        data_asset_name: Optional[str] = None,
        data_context_root_dir: Optional[Union[str, bytes, os.PathLike]] = None,
        data_context_config: Optional[DataContextConfig] = None,
        checkpoint_name: Optional[str] = None,
        checkpoint_config: Optional[CheckpointConfig] = None,
        checkpoint_kwargs: Optional[Dict] = None,
        fail_task_on_validation_failure: bool = True,
        validation_failure_callback: Optional[Callable[[CheckpointResult], None]] = None,
        return_json_dict: bool = False,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.run_name: Optional[str] = run_name
        self.conn_id: Optional[str] = conn_id
        self.base_directory: Optional[str] = base_directory
        self.file_regex: Optional[dict] = file_regex
        self.execution_engine: Optional[str] = (
            execution_engine if execution_engine else "PandasExecutionEngine"
        )
        self.store_backend_defaults: Optional[BaseStoreBackendDefaults] = store_backend_defaults
        self.batch_kwargs: Optional[Dict] = batch_kwargs if batch_kwargs else {}  # noqa
        self.expectation_suite_name: Optional[str] = expectation_suite_name
        self.data_connector: Optional[dict] = data_connector
        self.data_asset_name: Optional[str] = data_asset_name
        self.data_context_root_dir: Optional[Union[str, bytes, os.PathLike]] = data_context_root_dir
        self.data_context_config: DataContextConfig = data_context_config
        self.checkpoint_name: Optional[str] = checkpoint_name
        self.checkpoint_config: Optional[CheckpointConfig] = checkpoint_config if checkpoint_config else {}
        self.checkpoint_kwargs: Optional[Dict] = checkpoint_kwargs
        self.fail_task_on_validation_failure: Optional[bool] = fail_task_on_validation_failure
        self.validation_failure_callback: Optional[
            Callable[[CheckpointResult], None]
        ] = validation_failure_callback
        self.return_json_dict: bool = return_json_dict

        # Check that only one of the arguments is passed to set a data context
        if not bool(self.data_context_root_dir) ^ bool(self.data_context_config):
            raise ValueError("Exactly one of data_context_root_dir or data_context_config must be specified.")

        if bool(self.data_asset_name) and not bool(self.data_context_root_dir):
            raise ValueError("A data_context_root_dir must be specified with data_asset_name.")

        # Check that only one of the arguments is passed to set a checkpoint
        if not bool(self.checkpoint_name) and bool(self.checkpoint_config):
            raise ValueError(
                "Exactly one, or none, of checkpoint_name or checkpoint_config must be specified. If neither is"
                " specified, the default Checkpoint is used."
            )

        if self.base_directory and not self.file_regex:
            self.file_regex = {"group_names": ["data_asset_name"], "pattern": "(.*)"}

    def make_connection_string(self):
        """Builds connection strings based off existing Airflow connections."""
        if self.conn_type == "redshift" or self.conn_type == "postgres":
            return f"postgresql+psycopg2://{self.conn.login}:{self.conn.password}@{self.conn.host}:{self.conn.port}/{self.conn.schema}"  # noqa
        elif self.conn_type == "snowflake":
            return f"snowflake://{self.conn.login}:{self.conn.password}@{self.conn.extra_dejson['extra__snowflake__account']}.{self.conn.extra_dejson['extra__snowflake__region']}/{self.conn.extra_dejson['extra__snowflake__database']}/{self.conn.schema}?warehouse={self.conn.extra_dejson['extra__snowflake__warehouse']}&role={self.conn.extra_dejson['extra__snowflake__role']}"  # noqa
        elif self.conn_type == "gcpbigquery":
            return f"{self.conn.host}{self.conn.schema}"
        else:
            raise ValueError(f"Conn type: {self.conn_type} is not supported.")

    @property
    def default_datasource_config(self) -> Dict[str, Any]:
        """Builds default datasources for a default data context."""
        datasource_config = {"default_datasource": {"class_name": "Datasource"}}
        # Set default execution engine and data connectors based on whether
        # a connection or a directory is passed
        if self.conn:
            connection_string = self.make_connection_string()
            datasource_config["default_datasource"]["execution_engine"] = {
                "class_name": "SqlAlchemyExecutionEngine",
                "connection_string": connection_string,
            }
            datasource_config["default_datasource"]["data_connectors"] = {
                "default_runtime_data_connector_name": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["default_identifier_name"],
                },
                "default_inferred_data_connector_name": {
                    "class_name": "InferredAssetSqlDataConnector",
                    "include_schema_name": "false",
                },
            }
        elif self.base_directory:
            datasource_config["default_datasource"]["execution_engine"] = {
                "class_name": self.execution_engine
            }
            datasource_config["default_datasource"]["data_connectors"] = {
                "default_runtime_data_connector_name": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["default_identifier_name"],
                },
                "default_inferred_data_connector_name": {
                    "class_name": "InferredAssetFilesystemDataConnector",
                    "base_directory": self.base_directory,
                    "default_regex": self.file_regex,
                },
            }
        else:
            datasource_config["default_datasource"]["execution_engine"] = {
                "class_name": self.execution_engine,
            }
            datasource_config["default_datasource"]["data_connectors"] = {
                "default_runtime_data_connector_name": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["default_identifier_name"],
                },
                "default_inferred_data_connector_name": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["default_identifier_name"],
                },
            }
        # If data_connector is passed, allow it to override above defaults
        if self.data_connector:
            datasource_config["default_datasource"]["data_connectors"] = self.data_connector
        return datasource_config

    @property
    def default_data_context_config(self) -> DataContextConfig:
        """Builds a default data context config when one is not supplied."""
        config = DataContextConfig(
            config_version=3,
            store_backend_defaults=FilesystemStoreBackendDefaults(root_directory=self.data_context_root_dir),
        )
        config.datasources = self.default_datasource_config
        return config

    @property
    def default_batch_request(self) -> Dict[str, Any]:
        """Builds a default batch request for a default checkpoint."""
        batch_request = {
            "datasource_name": "default_datasource",
            "data_connector_name": "default_inferred_data_connector_name",
            "data_asset_name": self.data_asset_name,
        }
        # BigQuery needs a temp table to run on; it is assumed the table will
        # be named the same as the data asset but with an added _temp suffix
        if self.conn_type == "gcpbigquery":
            self.batch_kwargs["batch_spec_passthrough"] = {
                "bigquery_temp_table": f"{self.data_asset_name}_temp"
            }
        batch_request.update(self.batch_kwargs)
        return batch_request

    @property
    def default_action_list(self) -> Dict[str, Any]:
        """Builds a default action list for a default checkpoint."""
        action_list = [
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },
            {
                "name": "store_evaluation_params",
                "action": {"class_name": "StoreEvaluationParametersAction"},
            },
            {
                "name": "update_data_docs",
                "action": {"class_name": "UpdateDataDocsAction", "site_names": []},
            },
        ]
        if os.getenv("OPENLINEAGE_URL"):
            action_list.append(
                {
                    "name": "open_lineage",
                    "action": {
                        "class_name": "OpenLineageValidationAction",
                        "module_name": "openlineage.common.provider.great_expectations",
                        "openlineage_host": os.getenv("OPENLINEAGE_URL"),
                        "openlineage_apiKey": os.getenv("OPENLINEAGE_API_KEY"),
                        "openlineage_namespace": os.getenv("OPENLINEAGE_NAMESPACE"),
                        "job_name": f"validate_{self.task_id}",
                    },
                }
            )
        return action_list

    @property
    def default_checkpoint_config(self) -> CheckpointConfig:
        """Builds a default checkpoint with default values."""
        validations = [{"batch_request": self.default_batch_request}]

        return CheckpointConfig(
            name=self.checkpoint_name,
            config_version=1.0,
            template_name=None,
            module_name="great_expectations.checkpoint",
            class_name="Checkpoint",
            run_name_template=f"{self.task_id}_{datetime.now().strftime('%Y-%m-%d::%H:%M:%S')}",
            expectation_suite_name=self.expectation_suite_name,
            batch_request=None,
            action_list=self.default_action_list,
            evaluation_parameters={},
            runtime_configuration={},
            validations=validations,
            profilers=[],
            ge_cloud_id=None,
            expectation_suite_ge_cloud_id=None,
        )

    def execute(self, context: Dict[str, Any]) -> Union[CheckpointResult, Dict]:
        """
        Determines whether a context and checkpoint exist or need to be built, then
        runs the resulting checkpoint.
        """
        self.log.info("Running validation with Great Expectations...")
        self.conn = BaseHook.get_connection(self.conn_id) if self.conn_id else None
        self.conn_type = self.conn.conn_type if self.conn else None

        self.log.info("Instantiating Data Context...")
        if self.data_context_root_dir and not self.data_asset_name:
            self.data_context: BaseDataContext = ge.data_context.DataContext(
                context_root_dir=self.data_context_root_dir
            )
        elif self.data_context_config:
            self.data_context: BaseDataContext = BaseDataContext(project_config=self.data_context_config)
        else:
            self.data_context: BaseDataContext = BaseDataContext(
                project_config=self.default_data_context_config
            )

        self.log.info("Creating Checkpoint...")
        self.checkpoint: Checkpoint
        if self.checkpoint_name:
            self.checkpoint = self.data_context.get_checkpoint(name=self.checkpoint_name)
        elif self.checkpoint_config:
            self.checkpoint = instantiate_class_from_config(
                config=self.checkpoint_config.to_json_dict(),
                runtime_environment={"data_context": self.data_context},
                config_defaults={"module_name": "great_expectations.checkpoint"},
            )
        else:
            self.checkpoint_name = f"{self.expectation_suite_name}.chk"
            self.checkpoint = instantiate_class_from_config(
                config=self.default_checkpoint_config.to_json_dict(),
                runtime_environment={"data_context": self.data_context},
                config_defaults={"module_name": "great_expectations.checkpoint"},
            )

        self.log.info("Running Checkpoint...")
        if self.checkpoint_kwargs:
            result = self.checkpoint.run(**self.checkpoint_kwargs)
        else:
            result = self.checkpoint.run()
        self.log.info("GE Checkpoint Run Result:\n%s", result)
        self.handle_result(result)

        if self.return_json_dict:
            return result.to_json_dict()

        return result

    def handle_result(self, result: CheckpointResult) -> None:
        """Handle the given validation result.

        If the validation failed, this method will:

        - call `validation_failure_callback`, if set
        - raise an `airflow.exceptions.AirflowException`, if
          `fail_task_on_validation_failure` is `True`, otherwise, log a warning
          message

        If the validation succeeded, this method will simply log an info message.

        :param result: The validation result
        :type result: CheckpointResult
        """
        if not result["success"]:
            if self.validation_failure_callback:
                self.validation_failure_callback(result)
            if self.fail_task_on_validation_failure:
                statistics = result.statistics
                results = result.results
                raise AirflowException(
                    "Validation with Great Expectations failed.\n"
                    f"Stats: {str(statistics)}\n"
                    f"Results: {str(results)}"
                )
            else:
                self.log.warning(
                    "Validation with Great Expectations failed. "
                    "Continuing DAG execution because "
                    "fail_task_on_validation_failure is set to False."
                )
        else:
            self.log.info("Validation with Great Expectations successful.")
