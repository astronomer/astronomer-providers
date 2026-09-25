import os

import pytest

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
)
from airflow.utils import db
from airflow.utils.session import create_session
from airflow.utils.timezone import datetime

try:
    from airflow.models.xcom import XComModel as XCom
except ImportError:  # Airflow 2
    from airflow.models import XCom

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
    dag = DAG(TEST_DAG_ID, start_date=datetime(2022, 1, 1))
    yield dag


@pytest.fixture
def context():
    """
    Creates a context with default execution date.
    """
    context = {"execution_date": datetime(2015, 1, 1), "logical_date": datetime(2015, 1, 1)}
    yield context
