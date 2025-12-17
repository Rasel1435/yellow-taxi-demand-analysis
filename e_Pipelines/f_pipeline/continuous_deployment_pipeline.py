from zenml import pipeline
from zenml.config import DockerSettings
from zenml.constants import DEFAULT_SERVICE_START_STOP_TIMEOUT
from zenml.integrations.constants import MLFLOW
from zenml.integrations.mlflow.steps import mlflow_model_deployer_step
from typing import Annotated
from logs import configure_logger

logger = configure_logger()

# Import steps
from e_Pipelines.f_pipeline.ETLFeaturePipeline import run_pipeline
from e_Pipelines.h_steps.i_feature_target_spliting import split_data
from e_Pipelines.h_steps.j_model_train import train_model
from e_Pipelines.h_steps.k_evaluate_model import evaluate_model
from e_Pipelines.h_steps.l_deployment_trigger import trigger_deployment, DeploymentTrigger

from configs import config

container_settings = DockerSettings(required_integrations=[MLFLOW])


@pipeline(
    enable_cache=False,
    settings={"docker": container_settings}
)
def continuous_deployment(
    min_r2: Annotated[float, "Minimum R2 to trigger deployment"] = 0.90,
    max_mape: Annotated[float, "Maximum MAPE allowed to trigger deployment"] = 0.45,
    workers: Annotated[int, "Number of deployment workers"] = 1,
    timeout: Annotated[int, "Deployment timeout"] = DEFAULT_SERVICE_START_STOP_TIMEOUT,
):
    """
    End-to-end automated deployment pipeline:
    1. ETL / Feature pipeline
    2. Train RandomForest
    3. Evaluate model
    4. Trigger deployment if metrics thresholds are met
    5. Deploy model to MLflow
    """
    try:
        logger.info("==> Running ETL Feature Pipeline")
        # ETL returns 5 items: reduced_data, scaler, scaler_path, pca, pca_path
        reduced_data, _, scaler_path, _, pca_path = run_pipeline()

        logger.info("==> Splitting data into train and test sets")
        X_train, X_test, y_train, y_test = split_data(reduced_data)

        logger.info("==> Training RandomForest model")
        # Train model
        model = train_model(
            X_train=X_train,
            y_train=y_train,
            scaler_path=scaler_path,
            pca_path=pca_path,
            model_name=f'{config.MODEL_NAME}-RandomForest',
        )

        logger.info("==> Evaluating trained model")
        r2, mape = evaluate_model(model=model, X=X_test, y=y_test)
        logger.info("==> Evaluation metrics computed successfully.")

        # Trigger deployment decision based on thresholds
        deployment_decision = trigger_deployment(
            r2=r2,
            mape=mape,
            deployment_trigger=DeploymentTrigger(
                min_r2=min_r2,
                max_mape=max_mape
            )
        )

        # Deploy to MLflow if thresholds met
        mlflow_model_deployer_step(
            model=model,
            deploy_decision=deployment_decision,
            model_name=f'{config.MODEL_NAME}-RandomForest',
            workers=workers,
            mlserver=False,
            timeout=timeout
        )

        logger.info("==> Continuous deployment pipeline completed successfully.")

    except Exception as e:
        logger.error(f"Error in continuous_deployment pipeline: {e}")
        raise e


if __name__ == "__main__":
    continuous_deployment()



# Run pipeline from project root
# python -m e_Pipelines.f_pipeline.continuous_deployment_pipeline

