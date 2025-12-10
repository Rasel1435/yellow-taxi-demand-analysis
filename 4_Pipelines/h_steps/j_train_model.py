import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
sys.path.append(project_root)

import logging
import mlflow
import time
import pandas as pd
import mlflow.sklearn

from configs import config
from typing import Annotated
from zenml import step, client
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import RandomizedSearchCV
from sklearn.base import BaseEstimator
# from i_split import split_data

logger = logging.getLogger(__name__)

# ZenML experiment tracker
tracker = client.Client().active_stack.experiment_tracker

@step(
    name="Train RandomForest Model",
    experiment_tracker=tracker.name,
    enable_artifact_metadata=True,
    enable_artifact_visualization=True,
    enable_step_logs=True,
)
def train_model(
    X_train: Annotated[pd.DataFrame, "Training features"],
    y_train: Annotated[pd.Series, "Training target"],
    model_name: str = "RandomForestRegressor",
) -> Annotated[BaseEstimator, "trained_model"]:
    """
    Train RandomForest model using hyperparameter tuning with RandomizedSearchCV.

    Args:
        X_train: Training feature set
        y_train: Training target set
        model_name: Model name for MLflow logging

    Returns:
        Best trained RandomForest model (estimator)
    """
    try:
        logger.info("==> Processing train_model()")

        # Base model
        rf = RandomForestRegressor(random_state=42)

        # Hyperparameter search space
        param_dist = {
            "n_estimators": [200, 300, 400, 500],
            "max_depth": [10, 15, 20, 30, None],
            "min_samples_split": [2, 5, 10],
            "min_samples_leaf": [1, 2, 4],
            "max_features": ["sqrt", "log2", None],
            "bootstrap": [True, False],
        }

        # Randomized search
        search = RandomizedSearchCV(
            estimator=rf,
            param_distributions=param_dist,
            n_iter=40,
            cv=5,
            scoring="neg_mean_squared_error",
            n_jobs=-1,
            verbose=2,
            random_state=42
        )

        # Start MLflow run here
        with mlflow.start_run(run_name=f"{model_name}_training", nested=True):

            # add mlflow tag
            mlflow.set_tag("model_type", "RandomForestRegressor")

            # track training time
            start_time = time.time()
            # Fit model
            search.fit(X_train, y_train)
            training_time = time.time() - start_time
            mlflow.log_metric("training_time_sec", training_time)

            # best model + params
            best_model = search.best_estimator_
            best_params = search.best_params_

            # MLflow logging
            mlflow.log_params(best_params)

            # Log CV best score
            mlflow.log_metric("cv_best_score", search.best_score_)

            # Save model to MLflow
            mlflow.sklearn.log_model(
                best_model, 
                f"{config.MODEL_NAME}-RandomForest",
                input_example=X_train.iloc[:5]  # first 5 rows as example
            )

            logger.info(f"==> Successfully processed train_model()")
            logger.info(f"Best Parameters: {best_params}")
            logger.info(f"Training Time (sec): {training_time}")

            return best_model

    except Exception as e:
        logger.error(f"in train_model(): {e}")
        raise e
    finally:
        logger.info("==> Exiting train_model()")


# -----------------------------
# For testing
# -----------------------------
"""
if __name__ == '__main__':
    df = pd.read_csv('/media/sheikh/F262ADC762AD90C1/backup/ML/yellow-taxi-demand-analysis/3_Data/processed/g_2025_hourly_all_PCA_reduced.csv')
    X_train, X_test, y_train, y_test = split_data(df)
    # Train
    model = train_model(X_train=X_train, y_train=y_train, model_name=config.MODEL_NAME)
    print("Training completed. Model:", model)

    y_pred = model.predict(X_test)
    from sklearn.metrics import mean_squared_error, r2_score
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    print(f"Test MSE: {mse:.4f}, R2: {r2:.4f}")
"""