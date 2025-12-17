import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
current = os.path.dirname(os.path.abspath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
sys.path.append(os.path.join(current, "h_steps"))
sys.path.append(os.path.join(parent, "f_pipeline"))

from logs import configure_logger
logger = configure_logger()

from zenml import pipeline
from zenml.config import DockerSettings
from zenml.constants import DEFAULT_SERVICE_START_STOP_TIMEOUT
from zenml.integrations.constants import MLFLOW
from zenml.integrations.mlflow.steps import mlflow_model_deployer_step
from typing import Annotated

# Import steps
from f_pipeline.ETLFeaturePipeline import run_pipeline
from h_steps.i_feature_target_spliting import split_data
from h_steps.j_model_train import train_model
from h_steps.k_evaluate_model import evaluate_model
from h_steps.l_deployment_trigger import trigger_deployment, DeploymentTrigger

from configs import config

container_settings = DockerSettings(required_integrations=[MLFLOW])


@pipeline(
    enable_cache=False,
    settings={"docker": container_settings}
)
def continuous_deployment(
    min_r2: Annotated[float, "Minimum R2 to trigger deployment"] = 0.92,
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
        reduced_data, scaler_path, pca_path = run_pipeline()

        logger.info("==> Splitting data into train and test sets")
        X_train, X_test, y_train, y_test = split_data(reduced_data)

        logger.info("==> Training RandomForest model")
        model = train_model(
            X_train=X_train,
            y_train=y_train,
            scaler_path=scaler_path,
            pca_path=pca_path,
            model_name=f'{config.MODEL_NAME}-RandomForest',
        )

        logger.info("==> Evaluating trained model")
        r2, mape = evaluate_model(model=model, X=X_test, y=y_test)
        logger.info(f"==> Evaluation metrics: R2={r2:.4f}, MAPE={mape:.4f}")

        # Trigger deployment
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
            timeout=timeout,
            custom_tags={
                "scaler_path": scaler_path,
                "pca_path": pca_path,
                "r2_score": f"{r2:.4f}",
                "mape_score": f"{mape:.4f}"
            }
        )

        logger.info("==> Continuous deployment pipeline completed successfully.")

    except Exception as e:
        logger.error(f"Error in continuous_deployment pipeline: {e}")
        raise e


if __name__ == "__main__":
    continuous_deployment()

