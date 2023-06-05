from astronomer.providers.amazon.aws.operators.sagemaker import (
    SageMakerProcessingOperatorAsync,
    SageMakerTrainingOperatorAsync,
    SageMakerTransformOperatorAsync,
)


class TestSagemakerProcessingOperatorAsync:
    TASK_ID = "test_sagemaker_processing_operator"

    def test_sagemaker_processing_op_async(self):
        task = SageMakerProcessingOperatorAsync(
            config={},
            task_id=self.TASK_ID,
        )

        assert task.deferrable is True


class TestSagemakerTransformOperatorAsync:
    TASK_ID = "test_sagemaker_transform_operator"

    def test_sagemaker_transform_op_async(
        self,
    ):
        task = SageMakerTransformOperatorAsync(
            config={},
            task_id=self.TASK_ID,
        )

        assert task.deferrable is True


class TestSagemakerTrainingOperatorAsync:
    TASK_ID = "test_sagemaker_training_operator"

    def test_sagemaker_training_op_async(self):
        task = SageMakerTrainingOperatorAsync(
            config={},
            task_id=self.TASK_ID,
        )

        assert task.deferrable is True
