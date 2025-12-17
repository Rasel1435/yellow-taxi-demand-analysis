import mlflow
from zenml import step
from typing import Annotated
from pydantic import BaseModel
from logs import configure_logger

logger = configure_logger()


class DeploymentTrigger(BaseModel):
    """
    Deployment thresholds based on model evaluation metrics.
    Adjust min_r2 or max_mape as needed.
    """
    min_r2: float = 0.90        # minimum R2 score required
    max_mape: float = 0.45      # maximum MAPE allowed

@step(
    name='DeploymentTrigger',
    enable_step_logs=True,
    enable_artifact_metadata=True
)
def trigger_deployment(
    r2: Annotated[float, 'Model R2 score'],
    mape: Annotated[float, 'Model MAPE score'],
    deployment_trigger: Annotated[DeploymentTrigger, 'Deployment thresholds']
) -> Annotated[bool, 'Decision to deploy']:
    """
    Decides whether to deploy the trained model based on evaluation metrics.

    Logic:
        - R2 must be >= min_r2
        - MAPE must be <= max_mape
    """
    decision = (r2 >= deployment_trigger.min_r2) and (mape <= deployment_trigger.max_mape)

    # Determine stage
    if r2 >= 0.92:
        stage = "production"
    elif r2 >= 0.90:
        stage = "staging"
    else:
        stage = "rejected"

    # Log info
    if decision:
        logger.info(
            f"Deployment triggered! "
            f"(R2={r2:.4f} >= {deployment_trigger.min_r2}, "
            f"MAPE={mape:.4f} <= {deployment_trigger.max_mape})"
            f"Stage: {stage}"
        )
    else:
        logger.warning(
            f"Deployment NOT triggered. "
            f"(R2={r2:.4f}, MAPE={mape:.4f} vs thresholds "
            f"R2>={deployment_trigger.min_r2}, MAPE<={deployment_trigger.max_mape})"
            f"Stage: {stage}"
        )
    # Set MLflow stage tag
    mlflow.set_tag("stage", stage)

    return decision


# -----------------------------
# For independent testing
# -----------------------------
"""
if __name__ == "__main__":
    # Example with your current model metrics
    dt = DeploymentTrigger(min_r2=0.90, max_mape=0.45)
    deploy = trigger_deployment(r2=0.9168, mape=0.4198, deployment_trigger=dt)
    print("Deployment decision:", deploy)
"""
