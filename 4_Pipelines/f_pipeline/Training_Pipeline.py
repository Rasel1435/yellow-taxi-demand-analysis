import sys
import os
import joblib
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
current = os.path.dirname(os.path.abspath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
sys.path.append(os.path.join(current, "h_steps"))
sys.path.append(os.path.join(parent, "f_pipeline"))

from zenml import pipeline, client
from logs import configure_logger
logger = configure_logger()

# Import steps and ETL pipeline
from f_pipeline.ETLFeaturePipeline import run_pipeline
from h_steps.i_feature_target_spliting import split_data
from h_steps.j_model_train import train_model
from h_steps.k_evaluate_model import evaluate_model


@pipeline(
    name="TrainPipelineUberTaxiDemand",
    enable_step_logs=True,
    enable_artifact_metadata=True,
    enable_cache=True,
)
def trainPipeline():
    """
    Full training pipeline:
    - Runs ETL up to dimensionality reduction
    - Splits data into train/test
    - Trains RandomForest model
    - Evaluates model
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
            model_name='RandomForestRegressor',
        )

        logger.info("==> Evaluating trained model")
        r2, mape = evaluate_model(model=model, X=X_test, y=y_test)

        logger.info(f"==> Training pipeline completed | R2: {r2}, MAPE: {mape}")

        # Save model artifact
        model_path = f"3_Data/artifacts/trained_model.joblib"
        joblib.dump(model, model_path)
        logger.info(f"Model saved to {model_path}")

        # Return paths for inference
        return model, scaler_path, pca_path, r2, mape

    except Exception as e:
        logger.error(f"Error in trainPipeline(): {e}")
        raise e


if __name__ == "__main__":
    run = trainPipeline()