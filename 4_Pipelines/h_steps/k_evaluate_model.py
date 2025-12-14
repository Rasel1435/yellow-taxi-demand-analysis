import os
import sys
import pandas as pd
import numpy as np
import mlflow

from zenml import step, client
from typing import Annotated, Tuple
from sklearn.base import BaseEstimator
from sklearn.metrics import (
    mean_absolute_percentage_error,
    mean_squared_error,
    r2_score
)
from statsmodels.tools.eval_measures import rmse

# Add project root to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
sys.path.append(project_root)
from configs import config


from logs import configure_logger
logger = configure_logger()

# ZenML experiment tracker
tracker = client.Client().active_stack.experiment_tracker


# -------------------------------------------------------
# Utility: Compute AIC & BIC
# -------------------------------------------------------
def compute_aic_bic(y_true, y_pred, num_params: int):
    """
    Compute AIC and BIC for regression models.

    AIC = 2k - 2 ln(L)
    BIC = n ln(RSS/n) + k ln(n)

    where:
    k = number of parameters,
    n = number of observations,
    L = likelihood,
    RSS = residual sum of squares
    
    AIC → predictive performance
    BIC → simplicity preference

    y_true, y_pred: arrays
    num_params: model complexity (features + intercept)
    """

    y_true = np.asarray(y_true).ravel()
    y_pred = np.asarray(y_pred).ravel()

    n = len(y_true)
    resid = y_true - y_pred
    rss = np.sum(resid ** 2)

    # Numerical stability
    rss = max(rss, 1e-12)

    aic = 2 * num_params + n * np.log(rss / n)
    bic = n * np.log(rss / n) + num_params * np.log(n)

    return float(aic), float(bic)


# -------------------------------------------------------
# ZenML Evaluation Step
# -------------------------------------------------------
@step(
    name="Evaluate RandomForest Model",
    # experiment_tracker=tracker.name,
    enable_step_logs=True,
    enable_artifact_metadata=True,
    enable_artifact_visualization=True,
    enable_cache=True,
)
def evaluate_model(
    model: Annotated[BaseEstimator, "Trained model"],
    X: Annotated[pd.DataFrame, "Evaluation features"],
    y: Annotated[pd.Series, "Evaluation target"],
) -> Tuple[
    Annotated[float, "R2 Score"],
    Annotated[float, "MAPE Score"]
]:
    """
    Evaluate the trained model on validation/test data using multiple KPIs.
    Logs metrics to MLflow and returns R2 & MAPE.
    """

    try:
        logger.info("==> Starting evaluation...")

        # -----------------------------------------
        # Generate predictions
        # -----------------------------------------
        y_pred = model.predict(X)

        # -----------------------------------------
        # Metrics
        # -----------------------------------------
        mape = mean_absolute_percentage_error(y, y_pred)
        mse = mean_squared_error(y, y_pred)
        rmse_val = float(rmse(y, y_pred))
        r2 = r2_score(y, y_pred)

        # Model parameters = features + intercept term
        num_params = X.shape[1] + 1
        aic, bic = compute_aic_bic(y, y_pred, num_params)

        # -----------------------------------------
        # MLflow Logging
        # -----------------------------------------
        mlflow.log_metrics({
            "MAPE": mape,
            "MSE": mse,
            "RMSE": rmse_val,
            "R2": r2,
            "AIC": aic,
            "BIC": bic
        })

        mlflow.log_param("num_params", num_params)

        mlflow.set_tags({
            "stage": "evaluation",
            "metric_focus": "AIC_BIC",
            "model_type": type(model).__name__,
        })

        logger.info(
            f"Evaluation results | "
            f"R2={r2:.4f}, "
            f"MAPE={mape:.4f}, "
            f"RMSE={rmse_val:.4f}, "
            f"AIC={aic:.2f}, "
            f"BIC={bic:.2f}"
        )

        
        logger.info("==> Evaluation completed successfully.")

        return r2, mape

    except Exception as e:
        logger.error(f"Evaluation failed: {e}")
        raise e
    
# -------------------------------------------------------
# For testing the step independently
# -------------------------------------------------------
"""
if __name__ == "__main__":

    from i_feature_target_spliting import split_data
    from j_model_train import train_model

    # Load data
    df = pd.read_csv(
        "/media/sheikh/F262ADC762AD90C1/backup/ML/yellow-taxi-demand-analysis/3_Data/processed/g_2025_hourly_all_PCA_reduced.csv"
    )

    # Split data ONCE
    X_train, X_test, y_train, y_test = split_data(df)

    # Train model (ZenML-safe)
    model = train_model(
        X_train=X_train,
        y_train=y_train,
        model_name=config.MODEL_NAME,
    )

    print("Training completed.")

    # Evaluate model (ZenML-safe)
    r2, mape = evaluate_model(
        model=model,
        X=X_test,
        y=y_test,
    )

    print(f"Evaluation completed | R2={r2:.4f}, MAPE={mape:.4f}")
"""