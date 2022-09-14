from airflow.version import version as airflow_version
from packaging.version import Version

from astronomer.providers.utils.typing_compat import Context


def test_context_typing_compat():
    """
    Ensure the ``Context`` class is imported correctly in the typing_compat module based on the Apache Airflow
    version.

    The ``airflow.utils.context.Context`` class was not available in Apache Airflow until 2.3.3. Therefore,
    if the Apache Airflow version installed is older than 2.3.3, the ``Context`` class should be imported
    directly from the typing_compat module which contains a placeholder ``Context`` class.
    """
    if Version(airflow_version).release >= (2, 3, 3):
        assert Context.__module__ == "airflow.utils.context"
    else:
        assert Context.__module__ == "astronomer.providers.utils.typing_compat"
