import pandas as pd
from sklearn.preprocessing import StandardScaler
from zenml import step
from typing import Tuple

# -----------------------------------------------------
# Logger
# -----------------------------------------------------
from logs import configure_logger
logger = configure_logger()




@step(
    name="Normalize and Scale Features",
    enable_step_logs=True,
    enable_artifact_metadata=True
)
def scale_features(df: pd.DataFrame) -> Tuple[pd.DataFrame, StandardScaler]:
    """
    Normalizes and scales features using StandardScaler.

    Returns:
      - DataFrame containing timestamp + scaled features + taxi_demand
      - Fitted StandardScaler object
    """
    try:
        logger.info("==> Starting scale_features()")
        
        # Validate required columns
        required_cols = ['timestamp', 'taxi_demand']
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")

        # Split features and target
        X = df.drop(columns=['timestamp', 'taxi_demand'], errors='ignore')
        y = df['taxi_demand']
        timestamp = df['timestamp']

        # -------------------------------
        # DEBUG: Print before scaling
        # -------------------------------
        logger.info("==> Before scaling: \n%s", X.head())
        logger.info("==> Shape before scaling: %s", X.shape)

        # Scale features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        X_scaled_df = pd.DataFrame(X_scaled, columns=X.columns, index=df.index)

         # -------------------------------
        # DEBUG: Print after scaling
        # -------------------------------
        logger.info("==> After scaling: \n%s", X_scaled_df.head())
        logger.info("==> Shape after scaling: %s", X_scaled_df.shape)
        logger.info("==> Scaler mean values: %s", scaler.mean_)


        # Reattach timestamp and target
        X_scaled_df['timestamp'] = timestamp.values
        X_scaled_df['taxi_demand'] = y.values

        # Reorder columns (optional)
        cols = ['timestamp'] + list(X.columns) + ['taxi_demand']
        X_scaled_df = X_scaled_df[cols]

        logger.info("Normalization and Scaling completed successfully.")
        return X_scaled_df, scaler

    except Exception as e:
        logger.error(f"Error in scale_features: {e}")
        raise ValueError(f"Error in scale_features: {e}")



# For testing purposes
"""
if __name__ == "__main__":
    df = pd.read_csv('/media/sheikh/F262ADC762AD90C1/backup/ML/yellow-taxi-demand-analysis/3_Data/processed/e_2025_hourly_all_selected_features.csv')

    # Run scaling step
    scaled_df, fitted_scaler = scale_features(df)
    print(scaled_df)
    print(fitted_scaler.mean_)
"""