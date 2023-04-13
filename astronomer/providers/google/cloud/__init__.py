import pkg_resources

google_provider_version = pkg_resources.get_distribution("apache-airflow-providers-google").version
google_provider_version_lt_9 = pkg_resources.parse_version(google_provider_version).major < 9
