:py:mod:`core.example_dags.example_external_task`
=================================================

.. py:module:: core.example_dags.example_external_task

.. autoapi-nested-parse::

   Manually testing the async external task sensor requires a separate DAG for it to defer execution
   until the second DAG is complete.

       1. Add this file and "example_external_task_wait_for_me.py" to your local airflow/dags/
       2. Once airflow is running, select "Trigger Dag w/ Config" for DAG: "test_external_task_async"
       3. Copy the timestamp and hit the Trigger button
       4. Select "Trigger DAG w/ Config" for DAG: "test_external_task_async_waits_for_me"
       5. Paste timestamp and hit trigger button
       6. Confirm that "test_external_task_async" defers until "test_external_task_async_waits_for_me"
          successfully completes, then resumes execution and finishes without issue.



Module Contents
---------------

.. py:data:: ext_task_sensor
   

   

