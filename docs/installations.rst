Detailed Installation
---------------------
There are two ways to install astronomer-provider:

- Install and update all providers using `pip <https://pip.pypa.io/en/stable/getting-started/>`_:

.. code-block:: bash

    pip install astronomer-providers

- Install and update dependencies using extras using using `pip <https://pip.pypa.io/en/stable/getting-started/>`_. For example if a user wants to just use K8s async operator, they shouldn't need to install GCP, AWS, Databricks or Snowflake dependencies.

    - Install astronomer-provider for Amazon

        .. code-block:: bash

            pip install astronomer-providers[amazon]

    - Install astronomer-provider for Google

        .. code-block:: bash

            pip install astronomer-providers[google]


    - Install astronomer-provider for Google

        .. code-block:: bash

            pip install astronomer-providers[google]

    - Install astronomer-provider for Snowflake

        .. code-block:: bash

            pip install astronomer-providers[snowflake]

    - Install astronomer-provider for Databricks

        .. code-block:: bash

            pip install astronomer-providers[databricks]

    - Install astronomer-provider for Kubernetes

        .. code-block:: bash

            pip install astronomer-providers[cncf.kubernetes]
