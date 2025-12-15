import pandas as pd
import joblib
import datetime
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
# Add parent folder and steps to path
current = os.path.dirname(os.path.abspath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
sys.path.append(os.path.join(current, "h_steps"))
sys.path.append(os.path.join(parent, "f_pipeline"))

from logs import configure_logger
logger = configure_logger()

from zenml import pipeline
from h_steps.b_clean import clean_data
from h_steps.c_featureEngineering import feature_engineering
from h_steps.d_featuresSelection import SelectBestFeatures
from h_steps.m_load_preprocessors import load_preprocessors
from h_steps.n_predict import predict

@pipeline(enable_cache=False)
def inference_pipeline(
    raw_data: pd.DataFrame,
    model,
    scaler_path: str,
    pca_path: str
):
    data = clean_data(raw_data)
    data = feature_engineering(data)
    data = SelectBestFeatures(data)

    scaler, pca = load_preprocessors(
        scaler_path=scaler_path,
        pca_path=pca_path
    )

    data_scaled = scaler.transform(data)
    data_reduced = pca.transform(data_scaled)

    predictions = predict(model=model, X=data_reduced)
    return predictions


# ---------------------------------------------------------------------
# For Testing the inference pipeline
# ---------------------------------------------------------------------
if __name__ == "__main__":
    # Load sample raw data
    raw_data = pd.read_parquet("/media/sheikh/F262ADC762AD90C1/backup/ML/yellow-taxi-demand-analysis/3_Data/raw/yellow_tripdata_2025-01_january.parquet")
    
    # Load trained model
    model = joblib.load("3_Data/artifacts/trained_model.joblib")
    
    # Paths to saved scaler and PCA
    scaler_path = "3_Data/artifacts/scaler.joblib"
    pca_path = "3_Data/artifacts/pca.joblib"
    
    # Run inference pipeline
    pipeline_instance = inference_pipeline(
        raw_data=raw_data,
        model=model,
        scaler_path=scaler_path,
        pca_path=pca_path
    )
    
    predictions_artifact = pipeline_instance.run()
    predictions_df = predictions_artifact['predict'].read()
    print("Predictions:\n", predictions_df)







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