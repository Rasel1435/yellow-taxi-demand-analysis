import pandas as pd
from zenml import step
from typing import Tuple, Annotated
from sklearn.model_selection import train_test_split

# -----------------------------
# Logger
# -----------------------------
from logs import configure_logger
logger = configure_logger()


@step(
    name="Split Data",
    enable_artifact_metadata=True,
    enable_artifact_visualization=True,
    enable_step_logs=True,
    enable_cache=True,
)
def split_data(
    data: Annotated[pd.DataFrame, "Processed features and target"],
    test_size: float = 0.2,
    random_state: int = 42,
    drop_timestamp: bool = True
) -> Tuple[
    Annotated[pd.DataFrame, "X_train"],
    Annotated[pd.DataFrame, "X_test"],
    Annotated[pd.Series, "y_train"],
    Annotated[pd.Series, "y_test"]
]:
    """
    Splits the dataset into training and testing sets.

    Parameters
    ----------
    data : pd.DataFrame
        Input DataFrame with features and target 'taxi_demand'.
    test_size : float
        Fraction of data to use as test set.
    random_state : int
        Random seed for reproducibility.
    drop_timestamp : bool
        Whether to drop 'timestamp' from features.

    Returns
    -------
    X_train, X_test, y_train, y_test
    """

    logger.info("Splitting the data into train and test sets.")
    try:
        logger.info("==> Starting data split step")

        # Validate columns
        if "taxi_demand" not in data.columns:
            raise ValueError("Missing 'taxi_demand' column in input data")

        # Features & target
        X = data.drop(columns=["taxi_demand", "timestamp"], errors='ignore')
        y = data["taxi_demand"]

        # Train-test split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state
        )

        logger.info(f"==> Successfully split complete: X_train={X_train.shape}, X_test={X_test.shape}")
        logger.info(f"==> y_train={y_train.shape}, y_test={y_test.shape}")

        return X_train, X_test, y_train, y_test

    except Exception as e:
        logger.error(f"Error in split_data(): {e}", exc_info=True)
        raise e


# -----------------------------
# For testing
# -----------------------------
"""
if __name__ == '__main__':
    df = pd.read_csv('/media/sheikh/F262ADC762AD90C1/backup/ML/yellow-taxi-demand-analysis/3_Data/processed/g_2025_hourly_all_PCA_reduced.csv')
    X_train, X_test, y_train, y_test = split_data(df)
    print(X_train.shape, X_test.shape, y_train.shape, y_test.shape)
"""
