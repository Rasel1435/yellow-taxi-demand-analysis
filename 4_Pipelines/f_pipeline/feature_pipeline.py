import pandas as pd
import dask.dataframe as dd
import logging

from typing import Union
from zenml import step
from logs import configure_logger


# -----------------------------------------------------
# Logger setup
# -----------------------------------------------------
logger = configure_logger()


# -----------------------------------------------------
# Global variable for Dask/Pandas DataFrame
# -----------------------------------------------------
ddf: pd.DataFrame = None

# -----------------------------------------------------
# Optimize memory usage
# -----------------------------------------------------
def optimize_to_fit_memory(ddf: dd.DataFrame) -> dd.DataFrame:
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
        return ddf

# -----------------------------------------------------
# ZenML Step: Ingest Data
# -----------------------------------------------------
@step(name="Data Ingestion", enable_step_logs=True)
def ingest_data(DATA_SOURCE: str) -> Union[pd.DataFrame, None]:
    global ddf
    try:
        logger.info(f"Starting data ingestion from: {DATA_SOURCE}")

        # Read raw Parquet data
        ddf = dd.read_parquet(DATA_SOURCE, engine="pyarrow")

        # Infer month start
        start_of_month = DATA_SOURCE.split(".parquet")[0][-7:] + "-01 00:00:00"

        # Filter and select columns
        ddf = ddf.loc[
            ddf.tpep_pickup_datetime >= start_of_month,
            ["tpep_pickup_datetime", "passenger_count", "VendorID"]
        ]


        # Optimize memory
        ddf = ddf.map_partitions(optimize_to_fit_memory)
        

        # Set index, forward-fill, and resample hourly
        ddf = (
            ddf.set_index("tpep_pickup_datetime", sorted=True)
            .ffill()
            .resample("h")
            .agg({"passenger_count": "sum", "VendorID": "count"})
        )#.compute()

        # # Optimize memory
        # ddf = ddf.map_partitions(optimize_to_fit_memory)

        # Convert to Pandas DataFrame
        df = ddf.compute().reset_index()

        # Rename columns
        df.rename(
            columns={"passenger_count": "passenger_demand", "VendorID": "taxi_demand"},
            inplace=True
        )

        # Drop first/last row (edge effects)
        if df.shape[0] > 2:
            df.drop([0, df.shape[0]-1], inplace=True)

        # Update global ddf
        ddf = df

        logger.info(f"Data ingestion complete! Shape: {df.shape}")
        return df

    except Exception as e:
        logger.error(f"ingest_data() failed: {e}", exc_info=True)
        return None

# -----------------------------------------------------
# Example: call ingestion step locally
# -----------------------------------------------------
if __name__ == "__main__":
    df = ingest_data(
        DATA_SOURCE=r"../../3_Data/raw/yellow_tripdata_2025-01_january.parquet"
    )

    print(df.head())
    print(f"Data shape: {df.shape}" if df is not None else "Ingestion failed.")
