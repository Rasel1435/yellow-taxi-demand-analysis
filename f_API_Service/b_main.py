import os
import joblib
import pandas as pd
import numpy as np
from fastapi import FastAPI, HTTPException
from typing import List
from f_API_Service.a_schemas import TaxiFeatureInput, PredictionOutput

# Import your pipeline steps (ensure your PYTHONPATH includes the project root)
from e_Pipelines.h_steps.b_clean import clean_data
from e_Pipelines.h_steps.c_featureEngineering import feature_engineering

app = FastAPI(
    title="Yellow Taxi Demand Prediction Service",
    description="Real-time demand forecasting using RandomForest & PCA"
)

# ---------------------------------------------------------
# 1. LOAD ARTIFACTS ON STARTUP
# ---------------------------------------------------------
ARTIFACT_DIR = "d_Data/artifacts"

try:
    # Get the latest versions (you can hardcode or automate this)
    model = joblib.load(os.path.join(ARTIFACT_DIR, "yellow_taxi_demand_model_RandomForest_20251220_073717.joblib"))
    scaler = joblib.load(os.path.join(ARTIFACT_DIR, "scaler_20251220_073308.joblib"))
    pca = joblib.load(os.path.join(ARTIFACT_DIR, "pca_model_20251220_073327.joblib"))
    selected_features = joblib.load(os.path.join(ARTIFACT_DIR, "selected_features_20251220_073259.joblib"))
    print("✅ All ML artifacts loaded successfully.")
except Exception as e:
    print(f"❌ Error loading artifacts: {e}")

# ---------------------------------------------------------
# 2. HELPER: INFERENCE LOGIC
# ---------------------------------------------------------
def run_inference(raw_df: pd.DataFrame):
    # Convert string back to datetime so feature_engineering can extract Month/Day
    raw_df['tpep_pickup_datetime'] = pd.to_datetime(raw_df['tpep_pickup_datetime'])
    # 1. Cleaning & Feature Engineering
    df = clean_data(raw_df)
    df = feature_engineering(df)
    
    # 2. FEATURE ALIGNMENT
    # Ensure all columns from training are present
    for col in selected_features:
        if col not in df.columns:
            df[col] = 0
            
    # Keep only selected features in correct order
    X = df[selected_features]
    
    # 3. FIX: HANDLE THE NaN VALUES FROM LAG/WINDOWING
    # This is critical for single-row inference
    X = X.fillna(0) 
    
    # 4. Scale and Reduce
    X_scaled = scaler.transform(X)
    X_scaled_df = pd.DataFrame(X_scaled, columns=selected_features)
    X_pca = pca.transform(X_scaled_df)
    
    # 5. Predict
    preds = model.predict(X_pca)
    
    return df['timestamp'].astype(str).tolist(), preds.tolist()

# ---------------------------------------------------------
# 3. ENDPOINTS
# ---------------------------------------------------------
@app.post("/predict", response_model=List[PredictionOutput])
async def predict_demand(data: List[TaxiFeatureInput]):
    try:
        input_df = pd.DataFrame([item.dict() for item in data])
        timestamps, predictions = run_inference(input_df)
        
        # Ensure we are returning a clean list of dictionaries
        results = []
        for ts, p in zip(timestamps, predictions):
            results.append({
                "timestamp": ts, 
                "predicted_taxi_demand": float(p) # Ensure it's a standard Python float
            })
        return results

    except Exception as e:
        # This will print the ACTUAL error to your terminal if it crashes again
        print(f"DEBUG ERROR: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/")
def read_root():
    return {
        "project": "NYC Yellow Taxi Demand Analysis",
        "status": "online",
        "documentation": "/docs"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)


    # uvicorn f_API_Service.b_main:app --reload --port 8000