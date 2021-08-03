import os

import pytest

# These have to come before any Airflow imports
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"

from airflow.models import (
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
from airflow.utils.session import create_session


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
