import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
# Add parent folder and steps to path
current = os.path.dirname(os.path.abspath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
sys.path.append(os.path.join(current, "h_steps"))

from logs import configure_logger
logger = configure_logger()

from zenml import pipeline
from configs.config import DATA_SOURCE
from h_steps.a_ingest import ingest_data
from h_steps.b_clean import clean_data


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
        data = ingest_data(DATA_SOURCE=DATA_SOURCE)
        # Step 2: Clean
        cleaned_data = clean_data(data)
        logger.info(f'==> Successfully processed run_pipeline()')
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")



if __name__ == "__main__":
    run_pipeline()
