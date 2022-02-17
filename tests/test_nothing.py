from airflow.models.taskinstance import TaskInstance


def test_nothing(session):
    assert session.query(TaskInstance).count() == 0
