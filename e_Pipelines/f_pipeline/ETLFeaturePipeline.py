from zenml import pipeline
from configs.config import DATA_SOURCE
from e_Pipelines.h_steps.a_ingest import ingest_data
from e_Pipelines.h_steps.b_clean import clean_data
from e_Pipelines.h_steps.c_featureEngineering import feature_engineering
from e_Pipelines.h_steps.d_featuresSelection import SelectBestFeatures
from e_Pipelines.h_steps.g_NormalizeScaling import scale_features
from e_Pipelines.h_steps.h_dimensionalityReduction import ReduceDimensionality
from logs import configure_logger

logger = configure_logger()

# -----------------------------------------------------
# ETL / Feature Pipeline
# -----------------------------------------------------
@pipeline(
        name='ETLFeaturePipelineUberTaxiDemand',
        enable_step_logs=True,
        enable_cache=False,
        
    )

def run_pipeline():
    """
    Pipeline that runs all ETL / feature steps.
    """
    try:
        logger.info(f'==> Processing run_pipeline()')

        # Data ingestion
        data = ingest_data(DATA_SOURCE=DATA_SOURCE)

        # Data cleaning
        cleaned_data = clean_data(data)

        # Feature engineering
        featured_data = feature_engineering(cleaned_data)

        # Feature selection
        selected_features = SelectBestFeatures(featured_data)

        # Scaling / normalization
        scaled_features, scaler, scaler_path = scale_features(selected_features)

        # Dimensionality reduction
        reduced_data, pca, pca_path = ReduceDimensionality(scaled_features)

        logger.info(f'==> Successfully processed run_pipeline()')
        
        # Return all objects and paths
        return reduced_data, scaler, scaler_path, pca, pca_path
    
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise e



if __name__ == "__main__":
    run_pipeline()


# Run pipeline from project root
# python -m e_Pipelines.f_pipeline.ETLFeaturePipeline