import os

# Get the path where this project lives on ANY computer (Local or Render)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Point to the new 'static' folder inside your project
DATA_SOURCE = os.path.join(BASE_DIR, "d_Data", "static", "yellow_tripdata_2025-01_january.parquet")
# DATA_SOURCE = r'/media/sheikh/F262ADC762AD90C1/backup/ML/yellow-taxi-demand-analysis/d_Data/raw/yellow_tripdata_2025-01_january.parquet'
# DATA_SOURCE = r'/media/sheikh/F262ADC762AD90C1/backup/ML/yellow-taxi-demand-analysis/d_Data/processed/a_2025_hourly_all.parquet'
MODEL_NAME = "yellow_taxi_demand_model"
DEVELOPER_NAME = "Sheikh Rasel Ahmed"