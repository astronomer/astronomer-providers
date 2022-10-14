Sagemaker Operator Async
"""""""""""""""""""""""


SageMakerTransformOperatorAsync Starts a transform job and poll for the status asynchronously. A transform job uses a
trained model to get inferences on a dataset and saves these results to an Amazon S3 location that you specify.
:class:`~astronomer.providers.amazon.aws.operators.sagemaker.SageMakerTransformOperatorAsync`.

.. exampleinclude:: /../astronomer/providers/amazon/aws/example_dags/example_sagemaker.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_transform_async]
    :end-before: [END howto_operator_sagemaker_transform_async]


SageMakerTrainingOperatorAsync Starts a model training job and poll for the status asynchronously.
After training completes, Amazon SageMaker saves the resulting model artifacts
to an Amazon S3 location that you specify.
:class:`~astronomer.providers.amazon.aws.operators.sagemaker.SageMakerTransformOperatorAsync`.

.. exampleinclude:: /../astronomer/providers/amazon/aws/example_dags/example_sagemaker.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sagemaker_training_async]
    :end-before: [END howto_operator_sagemaker_training_async]
