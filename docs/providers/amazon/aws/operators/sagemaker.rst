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

SageMakerTransformOperatorAsync starts a transform job and polls for the status asynchronously. A transform job uses a
trained model to get inferences on a dataset and saves these results to an Amazon S3 location that you specify.
:class:`~astronomer.providers.amazon.aws.operators.sagemaker.SageMakerTransformOperatorAsync`.

.. exampleinclude:: /../astronomer/providers/amazon/aws/example_dags/example_sagemaker.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_transform_async]
    :end-before: [END howto_operator_sagemaker_transform_async]


SageMakerTrainingOperatorAsync starts a model training job and polls for the status asynchronously.
After training completes, Amazon SageMaker saves the resulting model artifacts
to an Amazon S3 location that you specify.
:class:`~astronomer.providers.amazon.aws.operators.sagemaker.SageMakerTransformOperatorAsync`.

.. exampleinclude:: /../astronomer/providers/amazon/aws/example_dags/example_sagemaker.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_training_async]
    :end-before: [END howto_operator_sagemaker_training_async]
