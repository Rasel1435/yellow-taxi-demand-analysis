import datetime
import joblib

from zenml import pipeline
from .logs import configure_logger
logger = configure_logger()

# Absolute imports from project root
from e_Pipelines.f_pipeline.ETLFeaturePipeline import run_pipeline
from h_steps.i_feature_target_spliting import split_data
from h_steps.j_model_train import train_model
from h_steps.k_evaluate_model import evaluate_model


@pipeline(
    name="TrainPipelineUberTaxiDemand",
    enable_step_logs=True,
    enable_artifact_metadata=True,
    enable_cache=False,
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
        # ETL returns reduced data, scaler, PCA, and paths
        reduced_data, scaler, scaler_path, pca, pca_path = run_pipeline()

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

        # Save trained model artifact
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        model_path = f"3_Data/artifacts/trained_model_{timestamp}.joblib"
        joblib.dump(model, model_path)
        logger.info(f"Model saved to {model_path}")

        return model, scaler_path, pca_path, r2, mape

    except Exception as e:
        logger.error(f"Error in trainPipeline(): {e}")
        raise e


if __name__ == "__main__":
    # Run pipeline from project root
    # python -m 4_Pipelines.f_pipeline.Training_Pipeline
    trainPipeline()
