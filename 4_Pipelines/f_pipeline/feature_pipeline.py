import sys
import os
import pandas as pd
import dask.dataframe as dd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from configs.config import DATA_SOURCE
from typing import Union, Tuple, Annotated
from logs import configure_logger
from feature_engine.datetime import DatetimeFeatures
from feature_engine.timeseries.forecasting import (
    LagFeatures, 
    WindowFeatures, 
    ExpandingWindowFeatures
)
from feature_engine.selection import SmartCorrelatedSelection, RecursiveFeatureElimination
from sklearn.tree import DecisionTreeRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA

from sklearn.model_selection import train_test_split


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
# Ingest Data
# -----------------------------------------------------
def ingest_data(DATA_SOURCE: str) ->  None:
    global ddf
    try:
        logger.info(f"===> Starting data ingestion from: {DATA_SOURCE}")
        # Read raw Parquet data
        ddf = dd.read_parquet(DATA_SOURCE, engine="pyarrow")

        # -----------------------------------------------------
        # Dynamically infer month start timestamp
        # Example: "yellow_tripdata_2025-01.parquet" → "2025-01-01 00:00:00"
        # -----------------------------------------------------
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

        logger.info(f"===> Data ingestion complete! Final head:\n{df.head()}")
        logger.info(f"===> Data ingestion complete! Shape: {df.shape}")
        logger.info("===> Successfully processed ingest_data()")
        return df

    except Exception as e:
        logger.error(f"ingest_data() failed: {e}", exc_info=True)
        return pd.DataFrame()
    


# -----------------------------------------------------
# Data Cleaning
# -----------------------------------------------------
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
        logger.info(f"Final head after cleaning:\n{data.head()}")
        logger.info(f"Final shape after cleaning: {data.shape}")
        logger.info("==> Successfully processed clean_data()")

        return data

    except Exception as e:
        logger.error(f"==> Error in clean_data(): {e}")
        return None
    


# -----------------------------------------------------
# add Temporal Features
# -----------------------------------------------------
def add_temporal_features(
        dataframe: pd.DataFrame, datetime_variable: str = 'timestamp'
        ) -> pd.DataFrame:
    """
    Adds temporal features to the dataframe using Feature-engine's DatetimeFeatures.

    Parameters:
    ----------
    dataframe : pd.DataFrame
        Input dataframe containing the datetime column.
    datetime_variable : str
        Name of the datetime column. Default is 'timestamp'.

    Returns:
    -------
    pd.DataFrame
        DataFrame with new temporal features appended.
    """
    try:
        logger.info("===> Starting add_temporal_features()")
        # Ensure the datetime column is of datetime type
        dataframe[datetime_variable] = pd.to_datetime(dataframe[datetime_variable])
        
        features_to_extract = [
            "month", "quarter","semester","year","week","day_of_week","day_of_month",
            "day_of_year","weekend","month_start","month_end","quarter_start",
            "quarter_end","year_start","year_end","leap_year","days_in_month","hour","minute","second"
        ]

        # Initialize DatetimeFeatures transformer
        dt_feat = DatetimeFeatures(
            variables=[datetime_variable],
            features_to_extract=features_to_extract
        )
        
        # Fit and transform
        temporal_features = dt_feat.fit_transform(dataframe[[datetime_variable]])
        
        # Merge new features back into the original dataframe
        dataframe = pd.concat([dataframe, temporal_features], axis=1)

        logger.info(f"===> Temporal features added: {list(temporal_features.columns)}")
        logger.info(f"===> Temporal features head:\n{temporal_features.head()}")
        logger.info(f"===> Data shape after adding temporal features: {dataframe.shape}")
        logger.info("===> Successfully processed add_temporal_features()")
        return dataframe

    except Exception as e:
        logger.error(f"Error in add_temporal_features(): {e}", exc_info=True)
        return dataframe


# -----------------------------------------------------
# add Lag Features
# -----------------------------------------------------
def add_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds lag features for 'passenger_demand' and 'taxi_demand'.

    Lag periods: 1, 2, 4, 8, 16, 24 hours
    Missing values are handled gracefully.
    """
    try:
        logger.info("===> Starting add_lag_features()")
        if df['timestamp'].dtype != 'datetime64[ns]':
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        lag_periods = [1, 2, 4, 8, 16, 24]
        lag_variables = ["passenger_demand", "taxi_demand"]
        
        # Initialize LagFeatures transformer
        lag_transformer = LagFeatures(
            variables=lag_variables,
            periods=lag_periods,
            sort_index=True,
            missing_values='ignore',  # Avoid errors for first few NaNs
            drop_original=False
        )
        
        # Fit & transform
        lag_df = lag_transformer.fit_transform(df[['timestamp'] + lag_variables])
        
        # Append lag columns to original df
        for col in lag_df.columns:
            if col not in df.columns:  # avoid overwriting
                df[col] = lag_df[col].values
        
        logger.info(f"===> Lag features added: {list(lag_df.columns)}")
        logger.info(f"===> Data head after adding lag features:\n{df.head()}")
        logger.info(f"===> Data shape after adding lag features: {df.shape}")
        logger.info("===> Successfully processed add_lag_features()")
        return df
    
    except Exception as e:
        logger.error(f"Error in add_lag_features(): {e}", exc_info=True)
        return df
    

# -----------------------------------------------------
# add Window Features
# -----------------------------------------------------
def add_window_features(
    df: pd.DataFrame,
    variables: list = ['passenger_demand', 'taxi_demand'],
    window: int = 7,
    functions: list = ['mean', 'std', 'median'],
) -> pd.DataFrame:
    """
    Adds rolling window features to the dataframe using Feature-engine's WindowFeatures.

    Parameters:
    - df: Input DataFrame with 'timestamp' column and target variables.
    - variables: List of numeric variables to create window features for. Default: ['passenger_demand', 'taxi_demand'].
    - window: Rolling window size (number of observations). Default: 7.
    - functions: List of aggregation functions. Default: ['mean', 'std', 'median'].

    Returns:
    - df: DataFrame with additional window features.
    """
    try:
        logger.info("===> Starting add_window_features()")
        # Ensure timestamp is datetime
        if df['timestamp'].dtype != 'datetime64[ns]':
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        # Default variables
        if variables is None:
            variables = ['passenger_demand', 'taxi_demand']

        # Initialize WindowFeatures transformer
        window_transformer = WindowFeatures(
            variables=variables,
            window=window,
            min_periods=1,  # handle small windows
            functions=functions,
            periods=1,      # lag the window by 1 to avoid lookahead bias
            freq=None,
            sort_index=True,
            missing_values='ignore',  # avoid errors for first few rows
            drop_original=False
        )

        # Fit & transform
        window_df = window_transformer.fit_transform(df[['timestamp'] + variables])

        # Append new window features to original df
        for col in window_df.columns:
            if col not in df.columns:  # avoid overwriting
                df[col] = window_df[col].values

        logger.info(f"===> Window features added: {list(window_df.columns[1:])}")
        logger.info(f"===> Data head after adding window features:\n{df.head()}")
        logger.info(f"===> Data shape after adding window features: {df.shape}")
        logger.info("===> Successfully processed add_window_features()")
        return df

    except Exception as e:
        logger.error(f"Error in add_window_features(): {e}", exc_info=True)
        return df


# -----------------------------------------------------
# add Expanding Window Features
# -----------------------------------------------------
def add_expanding_window_features(
    df: pd.DataFrame,
    variables: list = ['passenger_demand', 'taxi_demand'],
    functions: list = ['std'],
    min_periods: int = 1
) -> pd.DataFrame:
    """
    Adds expanding window features to the dataframe using Feature-engine's ExpandingWindowFeatures.

    Parameters:
    - df: Input DataFrame with 'timestamp' column and target variables.
    - variables: List of numeric variables to create expanding window features for. Default: ['passenger_demand', 'taxi_demand'].
    - functions: List of aggregation functions. Default: ['std'].
    - min_periods: Minimum number of observations in expanding window. Default: 1.

    Returns:
    - df: DataFrame with additional expanding window features.
    """
    try:
        logger.info("===> Starting add_expanding_window_features()")
        # Ensure timestamp is datetime
        if df['timestamp'].dtype != 'datetime64[ns]':
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        # Default variables
        if variables is None:
            variables = ['passenger_demand', 'taxi_demand']

        # Initialize ExpandingWindowFeatures transformer
        exp_transformer = ExpandingWindowFeatures(
            variables=variables,
            min_periods=min_periods,
            functions=functions,
            periods=1,           # lag by 1 to prevent lookahead
            freq=None,
            sort_index=True,
            missing_values='ignore',  # avoid errors for first few rows
            drop_original=False
        )

        # Fit & transform
        exp_df = exp_transformer.fit_transform(df[['timestamp'] + variables])

        # Append new expanding window features to original df
        for col in exp_df.columns:
            if col not in df.columns:  # avoid overwriting
                df[col] = exp_df[col].values

        logger.info(f"===> Expanding window features added: {list(exp_df.columns[1:])}")
        logger.info(f"===> Data head after adding expanding window features:\n{df.head()}")
        logger.info(f"===> Data shape after adding expanding window features: {df.shape}")
        logger.info("===> Successfully processed add_expanding_window_features()")
        return df

    except Exception as e:
        logger.error(f"Error in add_expanding_window_features(): {e}", exc_info=True)
        return df

# -----------------------------------------------------
# Lets call the all feature_engineering steps
# -----------------------------------------------------
def feature_engineering(df: pd.DataFrame) -> pd.DataFrame:
    # Convert Dask to Pandas
    if isinstance(df, dd.DataFrame):
        logger.info("Converting Dask DataFrame to Pandas...")
        df = df.compute()
    logger.info("Starting feature engineering pipeline")
    temp_df = add_temporal_features(dataframe=df)
    lagged_df = add_lag_features(temp_df)
    windowed_df = add_window_features(lagged_df)
    expanded_df = add_expanding_window_features(windowed_df)

    # -----------------------------
    # Handle NaNs created by feature engineering
    # -----------------------------
    lag_columns = [col for col in expanded_df.columns if "_lag_" in col]
    window_columns = [col for col in expanded_df.columns if "window" in col]
    exp_columns = [col for col in expanded_df.columns if "expanding" in col]
    
    expanded_df[lag_columns] = expanded_df[lag_columns].fillna(0)
    expanded_df[window_columns] = expanded_df[window_columns].bfill()
    expanded_df[exp_columns] = expanded_df[exp_columns].fillna(0)

    logger.info("Feature engineering pipeline completed")
    return expanded_df



# -----------------------------------------------------
# Feature Selection: Hybrid Approach
# -----------------------------------------------------
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

        logger.info(f"Final DataFrame head:\n{final_df.head()}")
        logger.info(f"Final DataFrame shape: {final_df.shape}")
        logger.info("==> Successfully finished SelectBestFeatures()")
        return final_df

    except Exception as e:
        logger.error(f"Error in SelectBestFeatures(): {e}")
        return None

# -----------------------------------------------------
# Normalize and Scale Features
# -----------------------------------------------------

def scale_features(
        df: pd.DataFrame
        ) -> Tuple[pd.DataFrame, StandardScaler]:
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


# -----------------------------------------------------
# Dimensionality Reduction
# -----------------------------------------------------

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

# -----------------------------------------------------
# split data into train and test sets
# -----------------------------------------------------
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

# -----------------------------------------------------
# Pipeline: Feature Pipeline
# -----------------------------------------------------
def feature_pipeline():
    logger.info("===> Processing Feature Pipeline()")

    raw_df = ingest_data(DATA_SOURCE=DATA_SOURCE)
    clean_df = clean_data(raw_df)
    feature_engineered_df = feature_engineering(clean_df)
    feature_selected_df = SelectBestFeatures(feature_engineered_df)
    scaled_features_df, scaler = scale_features(feature_selected_df)
    reduced_df = ReduceDimensionality(scaled_features_df)
    X_train, X_test, y_train, y_test = split_data(reduced_df)

    logger.info("===> Successfully processed Feature Pipeline()")
    return X_train, X_test, y_train, y_test

# -----------------------------------------------------
# Example: call ingestion step locally
# -----------------------------------------------------
if __name__ == "__main__":
    df = feature_pipeline()

    # print(df.head())
    # print(f"Data shape: {df.shape}")
