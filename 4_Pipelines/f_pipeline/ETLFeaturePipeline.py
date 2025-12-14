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
from h_steps.c_featureEngineering import feature_engineering
from h_steps.d_featuresSelection import SelectBestFeatures
from h_steps.g_NormalizeScaling import scale_features
from h_steps.h_dimensionalityReduction import ReduceDimensionality
from h_steps.i_feature_target_spliting import split_data
from h_steps.j_model_train import train_model
from h_steps.k_evaluate_model import evaluate_model


# -----------------------------------------------------
# ETL / Feature Pipeline
# -----------------------------------------------------
@pipeline(
        name='ETLFeaturePipelineUberTaxiDemand',
        enable_step_logs=True,
        enable_cache=True,
        
    )

def run_pipeline():
    """
    Pipeline that runs all ETL / feature steps.
    """
    try:
        logger.info(f'==> Processing run_pipeline()')
        data = ingest_data(DATA_SOURCE=DATA_SOURCE)
        cleaned_data = clean_data(data)
        featured_data = feature_engineering(cleaned_data)
        selected_features = SelectBestFeatures(featured_data)
        scaled_features, scaler = scale_features(selected_features)
        reduced_data = ReduceDimensionality(scaled_features)
        X_train, X_test, y_train, y_test = split_data(reduced_data)
        model = train_model(X_train=X_train, y_train=y_train, model_name='RandomForestRegressor')
        r2, mape = evaluate_model(model=model, X=X_test, y=y_test)

        logger.info(f'==> Successfully processed run_pipeline()')

        return model, r2, mape

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")



if __name__ == "__main__":
    run_pipeline()
