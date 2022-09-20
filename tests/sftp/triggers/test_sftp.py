from astronomer.providers.sftp.triggers.sftp import SFTPTrigger


def test_sftp_trigger_serialization():
    """
    Asserts that the SFTPTrigger correctly serializes its arguments and classpath.
    """
    trigger = SFTPTrigger(path="test/path/", sftp_conn_id="sftp_default", file_pattern="my_test_file")
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.sftp.triggers.sftp.SFTPTrigger"
    assert kwargs == {
        "path": "test/path/",
        "file_pattern": "my_test_file",
        "sftp_conn_id": "sftp_default",
        "poll_interval": 5.0,
    }
