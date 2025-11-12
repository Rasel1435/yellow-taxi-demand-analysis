import pandas as pd
from zenml import step
from typing import Union
from dask import dataframe as dd
from logs import configure_logger

logger = configure_logger()


@step(
    name="Data Cleaning",
    enable_step_logs=True,
    enable_artifact_metadata=True
)
def clean_data(data: Union[pd.DataFrame, dd.DataFrame]) -> Union[pd.DataFrame, dd.DataFrame, None]:
    """
    Clean the data by:
    - Dropping duplicates and null values
    - Converting datetime column
    - Renaming key columns
    - Removing extreme outliers
    """

    try:
        logger.info("==> Processing clean_data()")

        # Handle both Dask and Pandas
        is_dask = isinstance(data, dd.DataFrame)
        if is_dask:
            data = data.compute()
            logger.info("Converted Dask DataFrame to pandas for cleaning.")

        # -----------------------------------
        # Drop duplicates & NaNs
        # -----------------------------------
        data = data.drop_duplicates()
        data = data.dropna(axis=0, how="any")

        # -----------------------------------
        # Standardize datetime and columns
        # -----------------------------------
        if "tpep_pickup_datetime" in data.columns:
            data["timestamp"] = pd.to_datetime(data["tpep_pickup_datetime"], errors="coerce")
            data.drop(columns=["tpep_pickup_datetime"], inplace=True)

        data.rename(
            columns={
                "passenger_count": "passenger_demand",
                "VendorID": "taxi_demand",
            },
            inplace=True,
        )

        # -----------------------------------
        # Drop duplicates on timestamp
        # -----------------------------------
        before_dupes = len(data)
        data.drop_duplicates(subset=["timestamp"], inplace=True)
        after_dupes = len(data)
        logger.info(f"Removed {before_dupes - after_dupes} duplicate timestamps")

        # -----------------------------------
        # Handle Outliers (IQR method)
        # -----------------------------------
        for col in ["passenger_demand", "taxi_demand"]:
            if col in data.columns:
                Q1 = data[col].quantile(0.25)
                Q3 = data[col].quantile(0.75)
                IQR = Q3 - Q1
                lower = Q1 - 1.5 * IQR
                upper = Q3 + 1.5 * IQR
                before = len(data)
                data = data[(data[col] >= lower) & (data[col] <= upper)]
                after = len(data)
                logger.info(f"{col}: removed {before - after} outliers (IQR bounds [{lower:.2f}, {upper:.2f}])")

        # -----------------------------------
        # Final summary
        # -----------------------------------
        data.sort_values("timestamp", inplace=True)
        logger.info(f"Final shape after cleaning: {data.shape}")
        logger.info("==> Successfully processed clean_data()")

        return data

    except Exception as e:
        logger.error(f"==> Error in clean_data(): {e}")
        return None
    

# if __name__ == "__main__":
#     df = pd.read_csv("../../3_Data/processed/2025_hourly_all_EDA_cleaned.csv")
#     cleaned_data = clean_data(df)

#     if cleaned_data is not None:
#         print(cleaned_data.head())
#         print(f"Cleaned data shape: {cleaned_data.shape}")
#     else:
#         print("Data cleaning failed.")