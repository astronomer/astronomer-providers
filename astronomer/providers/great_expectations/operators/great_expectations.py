import os
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union

import great_expectations as ge
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from great_expectations.checkpoint import Checkpoint
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    CheckpointConfig,
    DataContextConfig,
)
from great_expectations.data_context.util import instantiate_class_from_config
from pandas import DataFrame

if TYPE_CHECKING:
    from airflow.utils.context import Context


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
    :param file_regex: The regex used to load files if not configured in a data context
    :type file_regex: Optional[Dict]
    :param execution_engine: The execution engine to use when running Great Expectations
    :type execution_engine: Optional[str]
    :param batch_request_extra: Additional arguments for a batch request
    :type batch_request_extra: Optional[Dict]
    :param expectation_suite_name: Name of the expectation suite to run if using a default Checkpoint
    :type expectation_suite_name: Optional[str]
    :param data_asset_name: The name of the table or dataframe that the default Data Context will load and default
        Checkpoint will run over
    :type data_asset_name: Optional[str]
    :param data_context_root_dir: Path of the great_expectations directory
    :type data_context_root_dir: Optional[str]
    :param data_context_config: A great_expectations `DataContextConfig` object
    :type data_context_config: Optional[DataContextConfig]
    :param runtime_data_source: A dataframe or SQL query passed in for run time checking
    :type runtime_data_source: Optional[Union[DataFrame, str]]
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
    :param use_open_lineage: If True (default), creates an OpenLineage action if an OpenLineage environment is found
    :type use_open_lineage: bool
    """

    ui_color = "#AFEEEE"
    ui_fgcolor = "#000000"

    def __init__(
        self,
        run_name: Optional[str] = None,
        conn_id: Optional[str] = None,
        file_regex: Optional[Dict[str, Any]] = None,
        execution_engine: Optional[str] = None,
        batch_request_extra: Optional[Dict[str, Any]] = None,
        expectation_suite_name: Optional[str] = None,
        data_asset_name: Optional[str] = None,
        data_context_root_dir: Optional[Union[str, bytes, os.PathLike[Any]]] = None,
        data_context_config: Optional[DataContextConfig] = None,
        runtime_data_source: Optional[Union[DataFrame, str]] = None,
        checkpoint_name: Optional[str] = None,
        checkpoint_config: Optional[CheckpointConfig] = None,
        checkpoint_kwargs: Optional[Dict[str, Any]] = None,
        validation_failure_callback: Optional[Callable[[CheckpointResult], None]] = None,
        fail_task_on_validation_failure: bool = True,
        return_json_dict: bool = False,
        use_open_lineage: bool = True,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.data_asset_name: Optional[str] = data_asset_name
        self.run_name: Optional[str] = run_name
        self.conn_id: Optional[str] = conn_id
        self.file_regex: Optional[Dict[str, Any]] = (
            file_regex if file_regex else {"group_names": ["data_asset_name"], "pattern": "(.*)"}
        )
        self.execution_engine: Optional[str] = (
            execution_engine if execution_engine else "PandasExecutionEngine"
        )
        self.batch_request_extra: Dict[str, Any] = batch_request_extra if batch_request_extra else {}  # noqa
        self.expectation_suite_name: Optional[str] = expectation_suite_name
        self.data_context_root_dir: Optional[Union[str, bytes, os.PathLike[Any]]] = data_context_root_dir
        self.data_context_config: DataContextConfig = data_context_config
        self.runtime_data_source = runtime_data_source
        self.checkpoint_name: Optional[str] = checkpoint_name
        self.checkpoint_config: Union[CheckpointConfig, Dict[Any, Any]] = (
            checkpoint_config if checkpoint_config else {}
        )
        self.checkpoint_kwargs: Optional[Dict[str, Any]] = checkpoint_kwargs
        self.fail_task_on_validation_failure: Optional[bool] = fail_task_on_validation_failure
        self.validation_failure_callback: Optional[
            Callable[[CheckpointResult], None]
        ] = validation_failure_callback
        self.return_json_dict: bool = return_json_dict
        self.use_open_lineage = use_open_lineage

        # Check that only one of the arguments is passed to set a data context
        if not (self.data_context_root_dir ^ self.data_context_config):
            raise ValueError("Exactly one of data_context_root_dir or data_context_config must be specified.")

        if not (self.runtime_data_source and self.conn_id):
            raise ValueError(
                "Exactly one, or neither, or runtime_data_source or conn_id may be specified. If neither is"
                " specified, the data_context_root_dir is used to find the data source."
            )

        # A data asset name is also used to determine if a runtime env will be used; if it is not passed in,
        # then the data asset name is assumed to be configured in the data context passed in.
        if (self.runtime_data_source or self.conn_id) and not self.data_asset_name:
            raise ValueError("A data_asset_name must be specified with a runtime_data_source or conn_id.")

        # Check that at most one of the arguments is passed to set a checkpoint
        if not (self.checkpoint_name and self.checkpoint_config):
            raise ValueError(
                "Exactly one, or neither, of checkpoint_name or checkpoint_config may be specified. If neither is"
                " specified, the default Checkpoint is used."
            )

        if not (self.checkpoint_name or self.checkpoint_config) and not self.expectation_suite_name:
            raise ValueError(
                "An expectation_suite_name must be supplied if neither checkpoint_name nor checkpoint_config are."
            )

        if type(self.checkpoint_config) == CheckpointConfig:
            self.checkpoint_config = self.checkpoint_config.to_json_dict()

    def make_connection_string(self) -> str:
        """Builds connection strings based off existing Airflow connections. Only supports necessary extras."""
        uri_string = ""
        if not self.conn:
            raise ValueError("No conn passed to operator.")
        if self.conn_type in ("redshift", "postgres", "mysql", "mssql"):
            odbc_connector = ""
            if self.conn_type in ("redshift", "postgres"):
                odbc_connector = "postgresql+psycopg2"
            elif self.conn_type == "mysql":
                odbc_connector = "mysql"
            else:
                odbc_connector = "mssql+pyodbc"
            uri_string = f"{odbc_connector}://{self.conn.login}:{self.conn.password}@{self.conn.host}:{self.conn.port}/{self.conn.schema}"  # noqa
        elif self.conn_type == "snowflake":
            uri_string = f"snowflake://{self.conn.login}:{self.conn.password}@{self.conn.extra_dejson['extra__snowflake__account']}.{self.conn.extra_dejson['extra__snowflake__region']}/{self.conn.extra_dejson['extra__snowflake__database']}/{self.conn.schema}?warehouse={self.conn.extra_dejson['extra__snowflake__warehouse']}&role={self.conn.extra_dejson['extra__snowflake__role']}"  # noqa
        elif self.conn_type == "gcpbigquery":
            uri_string = f"{self.conn.host}{self.conn.schema}"
        elif self.conn_type == "sqllite":
            uri_string = f"sqlite:///{self.conn.host}"
        # TODO: Add Athena and Trino support if possible
        else:
            raise ValueError(f"Conn type: {self.conn_type} is not supported.")
        return uri_string

    def build_runtime_datasources(self) -> Dict[str, Any]:
        """Builds runtime datasources based on Airflow connections or the given data_context_directory."""
        datasource_config: Dict[str, Any] = {"default_datasource": {"class_name": "Datasource"}}
        if self.conn:
            connection_string = self.make_connection_string()
            datasource_config["default_datasource"]["execution_engine"] = {
                "class_name": "SqlAlchemyExecutionEngine",
                "connection_string": connection_string,
            }
            datasource_config["default_datasource"]["data_connectors"] = {
                "default_configured_data_connector_name": {
                    "class_name": "ConfiguredAssetSqlDataConnector",
                    "include_schema_name": "false",
                },
            }
        # A dataframe or SQL query is passed
        elif self.runtime_data_source:
            datasource_config["default_datasource"]["execution_engine"] = {
                "class_name": self.execution_engine,
            }
            datasource_config["default_datasource"]["data_connectors"] = {
                "default_runtime_data_connector_name": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["default_identifier_name"],
                },
            }
        else:
            datasource_config["default_datasource"]["execution_engine"] = {
                "class_name": self.execution_engine
            }
            datasource_config["default_datasource"]["data_connectors"] = {
                "default_configured_data_connector_name": {
                    "class_name": "ConfiguredAssetFilesystemDataConnector",
                    "base_directory": self.data_context_root_dir,
                    "default_regex": self.file_regex,
                },
            }
        return datasource_config

    def build_runtime_env(self) -> Dict[str, Any]:
        """Builds the runtime_environment dict that overwrites all other configs."""
        runtime_env: Dict[str, Any] = {}
        runtime_env["datasources"] = self.build_runtime_datasources()
        return runtime_env

    def default_batch_request_dict(self) -> Dict[str, Any]:
        """Builds a default batch request for a default checkpoint."""
        data_connector_name = (
            "default_runtime_data_connector_name"
            if self.runtime_data_source
            else "default_inferred_data_connector_name"
        )
        batch_request = {
            "datasource_name": "default_datasource",
            "data_connector_name": data_connector_name,
            "data_asset_name": self.data_asset_name,
        }
        # BigQuery needs a temp table to run on; it is assumed the table will
        # be named the same as the data asset but with an added _temp suffix
        if self.conn_type == "gcpbigquery":
            self.batch_request_extra["batch_spec_passthrough"] = {
                "bigquery_temp_table": f"{self.data_asset_name}_temp"
            }
        batch_request.update(self.batch_request_extra)
        return batch_request

    def build_default_action_list(self) -> List[Dict[str, Any]]:
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

        if (
            os.getenv("AIRFLOW__LINEAGE__BACKEND") == "openlineage.lineage_backend.OpenLineageBackend"
            and self.use_open_lineage
        ):
            self.log.info(
                "Found OpenLineage Connection, automatically connecting... "
                "\nThis behavior may be turned off by setting use_open_lineage to False."
            )
            openlineage_host = os.getenv("OPENLINEAGE_URL")
            openlineage_api_key = os.getenv("OPENLINEAGE_API_KEY")
            openlineage_namespace = os.getenv("OPENLINEAGE_NAMESPACE")
            if not (openlineage_host and openlineage_api_key and openlineage_namespace):
                raise ValueError(
                    "Could not find one of OpenLineage host, API Key, or Namespace environment variables."
                    f"\nHost: {openlineage_host}\nAPI Key: {openlineage_api_key}\nNamespace: {openlineage_namespace}"
                )
            action_list.append(
                {
                    "name": "open_lineage",
                    "action": {
                        "class_name": "OpenLineageValidationAction",
                        "module_name": "openlineage.common.provider.great_expectations",
                        "openlineage_host": openlineage_host,
                        "openlineage_apiKey": openlineage_api_key,
                        "openlineage_namespace": openlineage_namespace,
                        "job_name": f"validate_{self.task_id}",
                    },
                }
            )
        return action_list

    def build_default_checkpoint_config(self) -> CheckpointConfig:
        """Builds a default checkpoint with default values."""
        validations = [{"batch_request": self.default_batch_request_dict}]
        self.run_name = (
            self.run_name
            if self.run_name
            else f"{self.task_id}_{datetime.now().strftime('%Y-%m-%d::%H:%M:%S')}"
        )
        return CheckpointConfig(
            name=self.checkpoint_name,
            config_version=1.0,
            template_name=None,
            module_name="great_expectations.checkpoint",
            class_name="Checkpoint",
            run_name_template=self.run_name,
            expectation_suite_name=self.expectation_suite_name,
            batch_request=None,
            action_list=self.build_default_action_list(),
            evaluation_parameters={},
            runtime_configuration={},
            validations=validations,
            profilers=[],
            ge_cloud_id=None,
            expectation_suite_ge_cloud_id=None,
        )

    def execute(self, context: "Context") -> Union[CheckpointResult, Dict[str, Any]]:
        """
        Determines whether a checkpoint exists or need to be built, then
        runs the resulting checkpoint.
        """
        self.log.info("Running validation with Great Expectations...")
        self.conn = BaseHook.get_connection(self.conn_id) if self.conn_id else None
        self.conn_type = self.conn.conn_type if self.conn else None

        self.log.info("Instantiating Data Context...")
        runtime_env: Dict[str, Any] = self.build_runtime_env() if self.data_asset_name else {}
        if self.data_context_root_dir:
            self.data_context = ge.data_context.DataContext(
                context_root_dir=self.data_context_root_dir, runtime_environment=runtime_env
            )
        else:
            self.data_context = BaseDataContext(
                project_config=self.data_context_config, runtime_environment=runtime_env
            )

        self.log.info("Creating Checkpoint...")
        self.checkpoint: Checkpoint
        if self.checkpoint_name:
            self.checkpoint = self.data_context.get_checkpoint(name=self.checkpoint_name)
        elif self.checkpoint_config:
            self.checkpoint = instantiate_class_from_config(
                config=self.checkpoint_config,
                runtime_environment={"data_context": self.data_context},
                config_defaults={"module_name": "great_expectations.checkpoint"},
            )
        else:
            self.checkpoint_name = f"{self.data_asset_name}.{self.expectation_suite_name}.chk"
            self.checkpoint = instantiate_class_from_config(
                config=self.build_default_checkpoint_config().to_json_dict(),
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
