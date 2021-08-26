import os

import pytest
from aioresponses import aioresponses

# These have to come before any Airflow imports
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"

from airflow.models import (
    DAG,
    DagModel,
    DagRun,
    DagTag,
    TaskInstance,
    TaskReschedule,
    Trigger,
    Variable,
    XCom,
)
from airflow.utils import db
from airflow.utils.dates import days_ago
from airflow.utils.session import create_session

TEST_DAG_ID = "unit_test_dag"


@pytest.fixture(scope="session", autouse=True)
def airflow_db():
    """
    Session-wide fixture that ensures the database is setup for tests.
    """
    db.resetdb()


@pytest.fixture
def session():
    """
    Creates a SQLAlchemy session.
    """
    with create_session() as session:
        yield session


@pytest.fixture(autouse=True)
def clean_db(session):
    """
    Clears test database after each test is run.
    """
    session.query(Trigger).delete()
    session.query(DagRun).delete()
    session.query(TaskInstance).delete()
    session.query(DagTag).delete()
    session.query(DagModel).delete()
    session.query(TaskReschedule).delete()
    session.query(Variable).delete()
    session.query(XCom).delete()


@pytest.fixture
def dag():
    """
    Creates a test DAG with default arguments.
    """
    args = {"owner": "airflow", "start_date": days_ago(0)}
    dag = DAG(TEST_DAG_ID, default_args=args)
    yield dag


@pytest.fixture
def aioresponse():
    """
    Creates an mock async API response.
    This comes from a mock library specific to the aiohttp package:
    https://github.com/pnuckowski/aioresponses

    """
    with aioresponses() as async_response:
        yield async_response
