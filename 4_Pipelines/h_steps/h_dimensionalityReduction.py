import pandas as pd
from zenml import step
from typing import Union
from dask import dataframe as dd
from sklearn.decomposition import PCA

from logs import configure_logger
logger = configure_logger()


@step(
    name="ReduceDimensionality",
    enable_step_logs=True,
    enable_artifact_metadata=True
)
def ReduceDimensionality(
    data: Union[pd.DataFrame, dd.DataFrame]
    ) -> Union[pd.DataFrame, None]:
    """
    Reduce dimensionality using PCA while preserving 95% variance.
    """

    try:
        logger.info("==> Starting PCA Dimensionality Reduction")

        # Convert Dask to Pandas if needed
        if isinstance(data, dd.DataFrame):
            logger.info("Converting Dask DataFrame to Pandas...")
            data = data.compute()

        # Separate features and target
        features = data.drop(columns=["taxi_demand"])
        target = data["taxi_demand"]

        
        # Keep only numeric columns
        numeric_features = features.select_dtypes(include=['float64', 'int64'])

        logger.info(f"==> Original Feature Shape: {numeric_features.shape}")

        # Apply PCA to preserve 95% variance
        pca = PCA(n_components=0.95, random_state=42)
        features_reduced = pca.fit_transform(numeric_features)

        logger.info(f"==> Reduced Feature Shape: {features_reduced.shape}")
        logger.info(f"==> Explained Variance Ratio: {pca.explained_variance_ratio_.sum():.4f}")

        # Create DataFrame with dynamic column names
        reduced_df = pd.DataFrame(
            features_reduced,
            columns=[f"PC{i+1}" for i in range(features_reduced.shape[1])]
        )

        # Add back target variable
        reduced_df["taxi_demand"] = target.values

        logger.info(f"==> Reduced DataFrame Head:\n{reduced_df.head()}")
        logger.info(f"==> Final Reduced DataFrame Shape: {reduced_df.shape}")
        logger.info(f"==> PCA Dimensionality Reduction Completed Successfully")

        return reduced_df

    except Exception as e:
        logger.error(f"Error in ReduceDimensionality step: {str(e)}")
        return None


# For testing purposes
"""
if __name__ == "__main__":
    df = pd.read_csv('/media/sheikh/F262ADC762AD90C1/backup/ML/yellow-taxi-demand-analysis/3_Data/processed/f_2025_hourly_all_selected_features_standardized.csv')
    reduced_df = ReduceDimensionality(df)
    print(reduced_df.head())
    print(reduced_df.shape)
"""