import pytest

from astronomer.providers.http.triggers.http import HttpTrigger


class TestHttpTrigger:
    @pytest.mark.parametrize(
        "input_conn,output_conn", ["http_default", "http_connection"]
    )
    def test_serialization(self, conn):
        """Asserts that the HttpTrigger correctly serializes its arguments and classpath."""
        trigger = HttpTrigger(
            endpoint="test-endpoint",
            http_conn_id=conn,
            method="GET",
            headers={"Content-Type": "application/json"},
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "astronomer.providers.http.triggers.http.HttpTrigger"
        assert kwargs == {
            "data": None,
            "endpoint": "test-endpoint",
            "extra_options": {},
            "headers": {"Content-Type": "application/json"},
            "http_conn_id": conn,
            "poke_interval": 5.0,
        }
