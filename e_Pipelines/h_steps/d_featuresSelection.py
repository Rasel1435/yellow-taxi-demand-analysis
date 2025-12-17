import joblib
import datetime
import pandas as pd

from dask import dataframe as dd
from typing import Union
from zenml import step
from feature_engine.selection import SmartCorrelatedSelection, RecursiveFeatureElimination
from sklearn.tree import DecisionTreeRegressor
from logs import configure_logger

logger = configure_logger()


@step(
    name="Select Best Features",
    enable_step_logs=True,
    enable_artifact_metadata=True
)
def SelectBestFeatures(
    df: Union[pd.DataFrame, dd.DataFrame]
    ) -> Union[pd.DataFrame, None]:
    """
    Performs hybrid feature selection using:
      - SmartCorrelatedSelection (correlation-based)
      - Recursive Feature Elimination (tree-based)

    Returns a DataFrame containing:
      timestamp + selected_features + taxi_demand
    """

    try:
        logger.info("==> Starting SelectBestFeatures()")
        
        # Convert Dask to Pandas
        if isinstance(df, dd.DataFrame):
            logger.info("Converting Dask DataFrame to Pandas...")
            df = df.compute()

        # ------------------------------------------
        # 1. Validate required columns
        # ------------------------------------------
        required_cols = ['timestamp', 'taxi_demand']
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")

        # ------------------------------------------
        # 2. Prepare X, y
        # ------------------------------------------
        logger.info("Step 1: Splitting X and y")

        X = df.drop(columns=['timestamp', 'passenger_demand', 'taxi_demand'], errors='ignore')
        y = df['taxi_demand']
        timestamp = df['timestamp']

        # ------------------------------------------
        # 3. Smart Correlated Selection
        # ------------------------------------------
        logger.info("Step 2: Running SmartCorrelatedSelection")

        scs = SmartCorrelatedSelection(
            method='pearson',
            threshold=0.5,
            missing_values='ignore',
            selection_method='variance',
            confirm_variables=False
        )
        scs_selected = set(scs.fit_transform(X).columns)

        logger.info(f"SCS selected features: {len(scs_selected)}")

        # ------------------------------------------
        # 4. Recursive Feature Elimination (RFE)
        # ------------------------------------------
        logger.info("Step 3: Running RecursiveFeatureElimination")

        rfe = RecursiveFeatureElimination(
            estimator=DecisionTreeRegressor(max_depth=3),
            scoring='r2',
            cv=3,
            threshold=0.01,
            variables=None,
            confirm_variables=False
        )

        rfe_selected = set(rfe.fit_transform(X, y).columns)

        logger.info(f"RFE selected features: {len(rfe_selected)}")

        # ------------------------------------------
        # 5. Combine features
        # ------------------------------------------
        logger.info("Step 4: Combining selected features")

        final_features = list(scs_selected.union(rfe_selected))
        logger.info(f"Total final selected features: {len(final_features)}")

        # ------------------------------------------
        # 6. Rebuild filtered DataFrame
        # ------------------------------------------
        logger.info("Step 5: Creating filtered dataframe")

        final_df = df[['timestamp'] + final_features + ['taxi_demand']]

        # Save selected features list
        version_stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        features_path = f'd_Data/artifacts/selected_features_{version_stamp}.joblib'
        joblib.dump(final_features, features_path)
        logger.info(f"Selected features saved to {features_path}")

        logger.info(f"==> Final Selected Features DataFrame Head:\n{final_df.head()}")
        logger.info(f"==> Final Selected Features DataFrame Shape: {final_df.shape}")
        logger.info(f"==> Successfully finished SelectBestFeatures()")

        return final_df

    except Exception as e:
        logger.error(f"Error in SelectBestFeatures(): {e}")
        return None
    

# -----------------------------------------------------
# For testing purposes
# -----------------------------------------------------
"""
if __name__ == "__main__":
    df = pd.read_csv("/media/sheikh/F262ADC762AD90C1/backup/ML/yellow-taxi-demand-analysis/d_Data/processed/d_2025_hourly_all_features.csv")
    selected_df = SelectBestFeatures(df=df)
    if selected_df is not None:
        logger.info("Selected features dataframe head:")
        print(selected_df.head())
    else:
        logger.error("Feature selection failed.")

"""