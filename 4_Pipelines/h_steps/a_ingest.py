import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
import logging
import pandas as pd
from typing import Union
from dask import dataframe as dd
from configs.config import DATA_SOURCE
from zenml import step

from logs import configure_logger
logger = configure_logger()


# -----------------------------------------------------
# Helper: Optimize Dask partitions for memory
# -----------------------------------------------------
def optimize_to_fit_memory(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Downcast numeric columns in a Dask DataFrame
    to reduce memory footprint efficiently.
    """
    try:
        type_map = {
            "int32": ["passenger_count"],
            "int16": ["VendorID"],
        }

        for dtype, cols in type_map.items():
            for col in cols:
                if col in ddf.columns:
                    ddf[col] = ddf[col].fillna(0).astype(dtype)

        logger.info("optimize_to_fit_memory() - successfully applied")
        return ddf

    except Exception as e:
        logger.error(f"optimize_to_fit_memory() failed: {e}")
        return ddf  # Return unchanged to avoid pipeline breakage


# -----------------------------------------------------
# ZenML Step: Ingest Data
# -----------------------------------------------------
@step(
    name="Data Ingestion",
    # description="Ingest and preprocess raw taxi trip data from Parquet source.",
    enable_step_logs=True,
    enable_artifact_metadata=True,
)
def ingest_data(DATA_SOURCE: str) -> Union[pd.DataFrame, None]:
    """
    Extracts and preprocesses raw taxi trip data from a Parquet source file.
    Steps:
    - Loads Parquet file using Dask
    - Filters data by inferred month start
    - Forward-fills missing values
    - Aggregates hourly passenger & taxi demand
    - Downcasts for memory efficiency
    - Returns final pandas DataFrame
    """

    try:
        logger.info(f"Starting data ingestion from: {DATA_SOURCE}")

        # -----------------------------------------------------
        # Read raw Parquet data
        # -----------------------------------------------------
        ddf = dd.read_parquet(DATA_SOURCE, engine="pyarrow")

        # -----------------------------------------------------
        # Dynamically infer month start timestamp
        # Example: "yellow_tripdata_2025-01.parquet" → "2025-01-01 00:00:00"
        # -----------------------------------------------------
        start_of_month = DATA_SOURCE.split(".parquet")[0][-7:] + "-01 00:00:00"

        # -----------------------------------------------------
        # Filter and select required columns
        # -----------------------------------------------------
        ddf = ddf.loc[
            ddf.tpep_pickup_datetime >= start_of_month,
            ["tpep_pickup_datetime", "passenger_count", "VendorID"],
        ]

        # -----------------------------------------------------
        # Optimize memory
        # -----------------------------------------------------
        ddf = ddf.map_partitions(optimize_to_fit_memory)


        # -----------------------------------------------------
        # Set index, fill missing, and resample hourly
        # -----------------------------------------------------
        ddf = (
            ddf.set_index("tpep_pickup_datetime", sorted=True)
            .ffill()
            .resample("h")
            .agg({"passenger_count": "sum", "VendorID": "count"})
        )#.compute()

        # -----------------------------------------------------
        # Optimize memory
        # -----------------------------------------------------
        # ddf = optimize_to_fit_memory(ddf)

        

        # -----------------------------------------------------
        # Compute from Dask → Pandas
        # -----------------------------------------------------
        df = ddf.compute().reset_index()

        # -----------------------------------------------------
        # Rename columns for clarity
        # -----------------------------------------------------
        df.rename(
            columns={
                "passenger_count": "passenger_demand",
                "VendorID": "taxi_demand",
            },
            inplace=True,
        )

        # -----------------------------------------------------
        # Drop first/last row (incomplete edge data)
        # -----------------------------------------------------
        if df.shape[0] > 2:
            df.drop([0, df.shape[0] - 1], inplace=True)

        # -----------------------------------------------------
        # Done
        # -----------------------------------------------------
        logger.info(f"Data ingestion complete! Final shape: {df.shape}")
        return df

    except Exception as e:
        logger.error(f"ingest_data() failed: {e}", exc_info=True)
        return None


if __name__ == "__main__":
    df = ingest_data(DATA_SOURCE=DATA_SOURCE)

    print(df.head())
    print(f"Data shape: {df.shape}" if df is not None else "Ingestion failed.")