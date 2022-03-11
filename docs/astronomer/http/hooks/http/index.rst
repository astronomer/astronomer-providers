:py:mod:`http.hooks.http`
=========================

.. py:module:: http.hooks.http


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   http.hooks.http.HttpHookAsync




.. py:class:: HttpHookAsync(method = 'POST', http_conn_id = default_conn_name, auth_type = aiohttp.BasicAuth, retry_limit = 3, retry_delay = 1.0)

   Bases: :py:obj:`airflow.hooks.base.BaseHook`

   Interact with HTTP servers using Python Async.

   :param method: the API method to be called
   :param http_conn_id: http connection id that has the base
       API url i.e https://www.google.com/ and optional authentication credentials. Default
       headers can also be specified in the Extra field in json format.
   :param auth_type: The auth type for the service
   :type auth_type: AuthBase of python aiohttp lib

   .. py:attribute:: conn_name_attr
      :annotation: = http_conn_id

      

   .. py:attribute:: default_conn_name
      :annotation: = http_default

      

   .. py:attribute:: conn_type
      :annotation: = http

      

   .. py:attribute:: hook_name
      :annotation: = HTTP

      

   .. py:method:: run(self, endpoint = None, data = None, headers = None, extra_options = None)
      :async:

      Performs an asynchronous HTTP request call

      :param endpoint: the endpoint to be called i.e. resource/v1/query?
      :param data: payload to be uploaded or request parameters
      :param headers: additional headers to be passed through as a dictionary
      :param extra_options: Additional kwargs to pass when creating a request.
          For example, ``run(json=obj)`` is passed as ``aiohttp.ClientSession().get(json=obj)``



