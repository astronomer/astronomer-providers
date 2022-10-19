Sagemaker Operator Async
""""""""""""""""""""""""


SageMakerProcessingOperatorAsync starts a processing job on AWS Sagemaker and polls for the status asynchronously.
A processing job is used to analyze data and to run your data processing workloads, such as feature
engineering, data validation, model evaluation, and model interpretation
:class:`~astronomer.providers.amazon.aws.operators.sagemaker.SageMakerProcessingOperatorAsync`.

.. exampleinclude:: /../astronomer/providers/amazon/aws/example_dags/example_sagemaker.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_processing_async]
    :end-before: [END howto_operator_sagemaker_processing_async]
