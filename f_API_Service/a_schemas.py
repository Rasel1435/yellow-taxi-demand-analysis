from pydantic import BaseModel
from typing import List

class TaxiFeatureInput(BaseModel):
    # These should match the raw features before your Feature Engineering step
    tpep_pickup_datetime: str  # e.g., "2025-01-01 12:00:00"
    passenger_count: int
    VendorID: int

class PredictionOutput(BaseModel):
    timestamp: str
    predicted_taxi_demand: float