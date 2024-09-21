import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import joblib

def ml_model_process(consolidated_file,annual_file):
    None
    # Step 1: Check for missing values (NaNs) in each column
    # print("Checking for missing values in each column:")
    # print(df.isnull().sum())

    # # Step 2: Check for infinite values in the DataFrame, but only in numeric columns
    # numeric_columns = ['open', 'high', 'low', 'close', 'adj_close', 'volume']
    # print("\nChecking for infinite values in numeric columns:")
    # for col in numeric_columns:
    #     if col in df.columns:
    #         infinite_count = np.isinf(df[col]).sum()
    #         print(f"{col}: {infinite_count} infinite values")

    # # Step 3: Remove duplicate rows
    # print("\nRemoving duplicate rows (if any):")
    # before_dedup = df.shape[0]
    # df.drop_duplicates(inplace=True)
    # after_dedup = df.shape[0]
    # print(f"Removed {before_dedup - after_dedup} duplicate rows.")

    # # Step 4: Ensure all numeric columns are properly converted to floats
    # for col in numeric_columns:
    #     df[col] = pd.to_numeric(df[col], errors='coerce')

    # # Step 5: Check for NaNs after conversion
    # print("\nChecking for NaNs after conversion to numeric:")
    # print(df.isnull().sum())

    # # Step 6: Check for extreme values in numeric columns
    # print("\nChecking for maximum and minimum values in numeric columns:")
    # print(df[numeric_columns].max())
    # print(df[numeric_columns].min())

    # # Step 7: Validate the final DataFrame
    # if df.isnull().values.any() or any(np.isinf(df[col]).any() for col in numeric_columns):
    #     raise ValueError("Data contains NaN or infinite values after processing.")

    # print("\nData validation passed. No NaN or infinite values detected.")


