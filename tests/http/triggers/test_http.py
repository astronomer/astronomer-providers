from astronomer.providers.http.triggers.http import HttpTrigger


def test_http_trigger_serialization():
    """
    Asserts that the HttpTrigger correctly serializes its arguments and classpath.
    """
    trigger = HttpTrigger(
        endpoint="test-endpoint",
        http_conn_id="http_default",
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
        "http_conn_id": "http_default",
        "poll_interval": 5.0,
    }
