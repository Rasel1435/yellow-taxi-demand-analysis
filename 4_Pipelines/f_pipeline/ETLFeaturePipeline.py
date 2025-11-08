import sys
import os

# Add parent folder and steps to path
current = os.path.dirname(os.path.abspath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
sys.path.append(os.path.join(current, "h_steps"))

from logs import configure_logger
logger = configure_logger()

from zenml import pipeline
from h_steps.ingest import ingest_data


# -----------------------------------------------------
# ETL / Feature Pipeline
# -----------------------------------------------------
@pipeline(
        name='ETLFeaturePipelineUberTaxiDemand',
        enable_step_logs=True
    )

def run_pipeline():
    """
    Pipeline that runs all ETL / feature steps.
    """
    try:
        logger.info(f'==> Processing run_pipeline()')
        # Step 1: Ingest
        data = ingest_data(DATA_SOURCE=r'../../3_Data/raw/yellow_tripdata_2025-01_january.parquet')
        logger.info(f'==> Successfully processed run_pipeline()')
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")



if __name__ == "__main__":
    run_pipeline()
