:py:mod:`http.triggers.http`
============================

.. py:module:: http.triggers.http


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   http.triggers.http.HttpTrigger




.. py:class:: HttpTrigger(endpoint, http_conn_id = 'http_default', method = 'GET', data = None, headers = None, extra_options = None, poll_interval = 5.0)

   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`

   A trigger that fires when the request to a URL returns a non-404 status code

   :param endpoint: The relative part of the full url
   :type endpoint: str
   :param http_conn_id: The HTTP Connection ID to run the sensor against
   :type http_conn_id: str
   :param method: The HTTP request method to use
   :type method: str
   :param data: payload to be uploaded or aiohttp parameters
   :type data: dict
   :param headers: The HTTP headers to be added to the GET request
   :type headers: a dictionary of string key/value pairs
   :param extra_options: Additional kwargs to pass when creating a request.
       For example, ``run(json=obj)`` is passed as ``aiohttp.ClientSession().get(json=obj)``
   :type extra_options: dict
   :param extra_options: Extra options for the 'requests' library, see the
       'requests' documentation (options to modify timeout, ssl, etc.)
   :type extra_options: A dictionary of options, where key is string and value
       depends on the option that's being modified.
   :param poll_interval: Time to sleep using asyncio
   :type poll_interval: float

   .. py:method:: serialize(self)

      Serializes HttpTrigger arguments and classpath.


   .. py:method:: run(self)
      :async:

      Makes a series of asynchronous http calls via a Databrick hook. It yields a Trigger if
      response is a 200 and run_state is successful, will retry the call up to the retry limit
      if the error is 'retryable', otherwise it throws an exception.
