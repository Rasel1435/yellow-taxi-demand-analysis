import os
import joblib
import pandas as pd
from fastapi import FastAPI, HTTPException
from typing import List
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware

from a_schemas import TaxiFeatureInput, PredictionOutput
from d_clean import clean_data
from e_featureEngineering import feature_engineering

ml_artifacts = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))   # f_API_Service
    PROJECT_ROOT = os.path.dirname(BASE_DIR)                # repo root
    ARTIFACT_DIR = os.path.join(PROJECT_ROOT, "d_Data", "artifacts")

    try:
        ml_artifacts["model"] = joblib.load(
            os.path.join(ARTIFACT_DIR, "yellow_taxi_demand_model_RandomForest_20251220_073717.joblib")
        )
        ml_artifacts["scaler"] = joblib.load(
            os.path.join(ARTIFACT_DIR, "scaler_20251220_073308.joblib")
        )
        ml_artifacts["pca"] = joblib.load(
            os.path.join(ARTIFACT_DIR, "pca_model_20251220_073327.joblib")
        )
        ml_artifacts["selected_features"] = joblib.load(
            os.path.join(ARTIFACT_DIR, "selected_features_20251220_073259.joblib")
        )
        print("All ML artifacts loaded successfully.")
    except Exception as e:
        print(f"Error loading artifacts: {e}")

    yield
    ml_artifacts.clear()

app = FastAPI(title="Yellow Taxi Demand Prediction Service", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def run_inference(raw_df: pd.DataFrame):
    if not ml_artifacts:
        raise RuntimeError("ML model is not loaded.")

    raw_df["tpep_pickup_datetime"] = pd.to_datetime(raw_df["tpep_pickup_datetime"])

    df = clean_data(raw_df)
    if df is None:
        raise ValueError("Data cleaning failed")

    df = feature_engineering(df)

    selected_features = ml_artifacts["selected_features"]
    for col in selected_features:
        if col not in df.columns:
            df[col] = 0

    X = df[selected_features].fillna(0)
    X_scaled = ml_artifacts["scaler"].transform(X)
    X_pca = ml_artifacts["pca"].transform(X_scaled)
    preds = ml_artifacts["model"].predict(X_pca)

    return df["timestamp"].astype(str).tolist(), preds.tolist()

@app.post("/predict", response_model=List[PredictionOutput])
async def predict_demand(data: List[TaxiFeatureInput]):
    try:
        input_df = pd.DataFrame([item.model_dump() for item in data])
        timestamps, predictions = run_inference(input_df)
        return [
            {"timestamp": ts, "predicted_taxi_demand": float(p)}
            for ts, p in zip(timestamps, predictions)
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health_check():
    ready = all(k in ml_artifacts for k in ["model", "scaler", "pca"])
    return {"status": "healthy" if ready else "initializing"}



if __name__ == "__main__":
    import uvicorn
    uvicorn.run("b_main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)))



# uvicorn f_API_Service.b_main:app --reload --port 8000