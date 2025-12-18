import os
import joblib
import datetime
import numpy as np
import pandas as pd
from typing import Tuple
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA

from zenml import pipeline, step

from e_Pipelines.h_steps.b_clean import clean_data
from e_Pipelines.h_steps.c_featureEngineering import feature_engineering
from e_Pipelines.h_steps.d_featuresSelection import SelectBestFeatures
from e_Pipelines.h_steps.n_predict import predict
from logs import configure_logger

logger = configure_logger()


# -------------------------------------------------
# STEP: Load raw data
# -------------------------------------------------
@step
def load_raw_data(path: str) -> pd.DataFrame:
    logger.info("Loading raw data")
    return pd.read_parquet(path)


# -------------------------------------------------
# STEP: Load trained model
# -------------------------------------------------
@step
def load_model(path: str):
    logger.info("Loading trained model")
    return joblib.load(path)


# -------------------------------------------------
# STEP: Load scaler & PCA
# -------------------------------------------------
@step
def load_preprocessors(
    scaler_path: str,
    pca_path: str,
) -> Tuple[StandardScaler, PCA]:
    logger.info("Loading scaler and PCA")
    scaler = joblib.load(scaler_path)
    pca = joblib.load(pca_path)
    return scaler, pca


# -------------------------------------------------
# STEP: Align features with training
# -------------------------------------------------
@step
def align_features(
    data: pd.DataFrame,
    selected_features_path: str
) -> pd.DataFrame:
    logger.info("Aligning features to match training")
    selected_features = joblib.load(selected_features_path)

    # Add missing features with default 0
    for col in selected_features:
        if col not in data.columns:
            data[col] = 0.0

    # Keep only selected features in correct order
    data = data[selected_features]
    return data


# -------------------------------------------------
# STEP: Apply scaling + PCA
# -------------------------------------------------
@step
def apply_preprocessors(
    data: pd.DataFrame,
    scaler,
    pca,
):
    logger.info("Applying scaler and PCA")
    data_scaled = scaler.transform(data)
    data_reduced = pca.transform(data_scaled)
    return data_reduced


# -------------------------------------------------
# PIPELINE: Inference
# -------------------------------------------------
@pipeline(enable_cache=False)
def inference_pipeline(
    raw_data_path: str,
    model_path: str,
    scaler_path: str,
    pca_path: str,
    selected_features_path: str,
):
    # Load
    raw_data = load_raw_data(raw_data_path)
    model = load_model(model_path)
    scaler, pca = load_preprocessors(scaler_path, pca_path)

    # Preprocessing
    data = clean_data(raw_data)
    data = feature_engineering(data)
    data = SelectBestFeatures(data)

    # Align features to training
    data = align_features(data, selected_features_path)

    # Transform
    data_reduced = apply_preprocessors(data, scaler, pca)

    # Predict
    predictions = predict(model=model, X=data_reduced)
    return predictions


# -------------------------------------------------
# Run pipeline
# -------------------------------------------------
if __name__ == "__main__":
    # Directly call the pipeline
    predictions = inference_pipeline(
        raw_data_path="/media/sheikh/F262ADC762AD90C1/backup/ML/yellow-taxi-demand-analysis/d_Data/raw/yellow_tripdata_2025-01_january.parquet",
        model_path="/media/sheikh/F262ADC762AD90C1/backup/ML/yellow-taxi-demand-analysis/d_Data/artifacts/yellow_taxi_demand_model_RandomForest_20251217_222930.joblib",
        scaler_path="/media/sheikh/F262ADC762AD90C1/backup/ML/yellow-taxi-demand-analysis/d_Data/artifacts/scaler_20251217_222531.joblib",
        pca_path="/media/sheikh/F262ADC762AD90C1/backup/ML/yellow-taxi-demand-analysis/d_Data/artifacts/pca_model_20251217_222550.joblib",
        selected_features_path="/media/sheikh/F262ADC762AD90C1/backup/ML/yellow-taxi-demand-analysis/d_Data/artifacts/selected_features_20251217_222521.joblib",
    )

    # Convert predictions to DataFrame
    predictions_array = np.ravel(predictions)
    predictions_df = pd.DataFrame(predictions_array, columns=["predictions"])

    # Save to your desired folder
    save_dir = "/media/sheikh/F262ADC762AD90C1/backup/ML/yellow-taxi-demand-analysis/d_Data/predictions"
    os.makedirs(save_dir, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(save_dir, f"predictions_{timestamp}.csv")
    predictions_df.to_csv(file_path, index=False)

    print(f"Predictions saved to {file_path}")
    print(predictions_df.head())








# Run pipeline from project root
# python -m e_Pipelines.f_pipeline.inference





"""
    inference.py pipeline:

    Purpose: Predict on new data using an existing, deployed model.

    What it does:
        1. Takes new raw data.
        2. Loads previously saved scaler & PCA (so preprocessing matches training).
        3. Runs the preprocessing steps (cleaning, features, selection, scaling, dimensionality reduction).
        4. Loads your already trained model.
        5. Generates predictions on the new data.

    When you run it: You run this when you don’t want to retrain but just want to get predictions on
    new incoming data.

    Output: Predictions for your new dataset. No retraining occurs.
"""