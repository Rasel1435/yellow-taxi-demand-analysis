import pandas as pd
import matplotlib.pyplot as plt

from dask import dataframe as dd
from zenml import step
from logs import configure_logger
from feature_engine.datetime import DatetimeFeatures
from feature_engine.timeseries.forecasting import (
    LagFeatures, 
    WindowFeatures, 
    ExpandingWindowFeatures
)
# -----------------------------------------------------
# Logger setup
# -----------------------------------------------------
logger = configure_logger()


# -----------------------------------------------------
# ZenML Step: add Temporal Features
# -----------------------------------------------------
@step(
    name="Add Temporal Features",
    enable_step_logs=True, 
    enable_artifact_metadata=True)

def add_temporal_features(
    dataframe: pd.DataFrame,
    datetime_variable: str = 'timestamp'
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

        logger.info(f"Temporal features added: {list(temporal_features.columns)}")
        logger.info(f"Data shape after adding temporal features: {dataframe.shape}")
        logger.info("Successfully processed add_temporal_features()")
        return dataframe

    except Exception as e:
        logger.error(f"Error in add_temporal_features(): {e}", exc_info=True)
        return dataframe


# -----------------------------------------------------
# ZenML Step: add Lag Features
# -----------------------------------------------------
@step(
    name="Add Lag Features",
    enable_step_logs=True,
    enable_artifact_metadata=True
    )

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
        
        logger.info(f"Lag features added: {list(lag_df.columns)}")
        logger.info(f"Data shape after adding lag features: {df.shape}")
        logger.info("Successfully processed add_lag_features()")
        return df
    
    except Exception as e:
        logger.error(f"Error in add_lag_features(): {e}", exc_info=True)
        return df
    

# -----------------------------------------------------
# ZenML Step: add Window Features
# -----------------------------------------------------
@step(
    name="Add Window Features",
    enable_step_logs=True,
    enable_artifact_metadata=True
    )

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

        logger.info(f"Window features added: {list(window_df.columns[1:])}")
        logger.info(f"Data shape after adding window features: {df.shape}")
        logger.info("Successfully processed add_window_features()")
        return df

    except Exception as e:
        logger.error(f"Error in add_window_features(): {e}", exc_info=True)
        return df


# -----------------------------------------------------
# ZenML Step: add Expanding Window Features
# -----------------------------------------------------
@step(
    name="Add Expanding Window Features",
    enable_step_logs=True,
    enable_artifact_metadata=True
    )

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

        logger.info(f"Expanding window features added: {list(exp_df.columns[1:])}")
        logger.info(f"===> Data shape after adding expanding window features: {df.shape}")
        logger.info("===> Successfully processed add_expanding_window_features()")
        return df

    except Exception as e:
        logger.error(f"Error in add_expanding_window_features(): {e}", exc_info=True)
        return df


# -----------------------------------------------------
# ZenML Pipeline: lets call the steps
# -----------------------------------------------------
@step(
    name="Feature Engineering Pipeline",
    enable_step_logs=True,
    enable_artifact_metadata=True
    )

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

    logger.info(f"==> Final feature engineered DataFrame Head:\n{expanded_df.head()}")
    logger.info(f"==> Final feature engineered DataFrame Shape: {expanded_df.shape}")
    logger.info(f"==> Feature engineering pipeline completed")
    
    return expanded_df


# -----------------------------------------------------
# For testing purposes
# -----------------------------------------------------
"""
if __name__ == "__main__":
    df = pd.read_csv("/media/sheikh/F262ADC762AD90C1/backup/ML/yellow-taxi-demand-analysis/d_Data/processed/c_2025_hourly_all_cleaned.csv")
    cleaned_data = feature_engineering(df)

    if cleaned_data is not None:
        logger.info(f"Final cleaned data shape: {cleaned_data.shape}")
        print(cleaned_data.head())
    else:
        logger.error("Feature engineering failed.")

"""