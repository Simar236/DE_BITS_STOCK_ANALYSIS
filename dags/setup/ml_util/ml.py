import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import joblib
from setup.util_functions.load_file import insert_dataframe_postgres

def ml_model_process(consolidated_file, annual_file):
    print("ml")
    predicted_stock_data_file_path = 'dags/consolidated_stock_data/predicted_stock_data.csv'
    model_path = 'dags/consolidated_stock_data/model/annual_cumulative_return_predictor.pkl'
    
    consolidated_df = pd.read_csv(consolidated_file)
    
    # Feature engineering
    consolidated_df['price_range'] = consolidated_df['high'] - consolidated_df['low']
    consolidated_df['daily_return'] = (consolidated_df['close'] - consolidated_df['open']) / consolidated_df['open']
    consolidated_df['volume_change'] = consolidated_df['volume'].pct_change()
    consolidated_df.fillna(0, inplace=True)
    
    # Calculate `annual_cumulative_return`
    consolidated_df['annual_cumulative_return'] = consolidated_df.groupby('stock_symbol', group_keys=False)['adj_close'].apply(
        lambda x: x.pct_change().cumsum().fillna(0)
    )

    # Aggregate data yearly
    annual_data = consolidated_df.groupby(['stock_symbol', pd.to_datetime(consolidated_df['date']).dt.year]).agg({
        'open': 'mean',
        'high': 'mean',
        'low': 'mean',
        'close': 'mean',
        'adj_close': 'mean',
        'volume': 'mean',
        'price_range': 'mean',
        'daily_return': 'sum',
        'volume_change': 'sum',
        'annual_cumulative_return': 'last'
    }).reset_index()

    # Drop rows with NaN
    annual_data.dropna(inplace=True)

    # Check if annual_data is empty
    if annual_data.empty:
        print("No data available after aggregation. Please check the input data.")
        return

    # Prepare features and target variable
    X = annual_data.drop(['stock_symbol', 'date', 'annual_cumulative_return'], axis=1)
    y = annual_data['annual_cumulative_return']

    # Print shapes for debugging
    print(f"Shape of X: {X.shape}")
    print(f"Shape of y: {y.shape}")

    # Check if X or y is empty
    if X.empty or y.empty:
        print("X or y is empty. Cannot proceed with model training.")
        return

    # Split data into train and test sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Check if train and test sets are empty
    if X_train.empty or X_test.empty:
        print("Training or testing data is empty after splitting. Please check the data.")
        return

    # Train the model
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    # Test the model
    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    print(f'Mean Squared Error: {mse}')

    # Save the trained model
    joblib.dump(model, model_path)

    # Future prediction on new data (next year's data)
    new_data = pd.read_csv(annual_file)

    # Calculate price_range and handle division by zero for daily_return
    new_data['price_range'] = new_data['high'] - new_data['low']
    new_data['daily_return'] = (new_data['close'] - new_data['open']) / new_data['open'].replace(0, np.nan)

    # Calculate volume_change and replace NaN with 0
    new_data['volume_change'] = new_data['volume'].pct_change().fillna(0)

    # Check for NaN values after calculations
    if new_data.isnull().values.any():
        print("NaN values found in new_data before aggregation!")
        new_data.fillna(0, inplace=True)  # or you could drop them

    # Aggregate yearly
    new_annual_data = new_data.groupby(['stock_symbol', pd.to_datetime(new_data['date']).dt.year]).agg({
        'open': 'mean',
        'high': 'mean',
        'low': 'mean',
        'close': 'mean',
        'adj_close': 'mean',
        'volume': 'mean',
        'price_range': 'mean',
        'daily_return': 'sum',
        'volume_change': 'sum'
    }).reset_index()

    # Check for NaN values in new_annual_data
    if new_annual_data.isnull().values.any():
        print("NaN values found in new_annual_data after aggregation!")
        new_annual_data.fillna(0, inplace=True)

    # Prepare input for prediction
    X_new = new_annual_data.drop(['stock_symbol', 'date'], axis=1)

    # Check if input to model contains NaN values
    if X_new.isnull().values.any():
        print("Input to model contains NaN values.")
        return

    # Use the saved model for prediction
    model = joblib.load(model_path)
    new_predictions = model.predict(X_new)

    # Get the max year from the consolidated data
    max_year = pd.to_datetime(consolidated_df['date']).dt.year.max()

    # Add predictions to the new_annual_data with the next year
    new_annual_data['predicted_annual_cumulative_return'] = new_predictions
    new_annual_data['predicted_year'] = max_year + 1

    # Print stock_symbol, predicted year, and predicted annual_cumulative_return
    result_df = new_annual_data[['stock_symbol', 'predicted_year', 'predicted_annual_cumulative_return']]
    print(f"ml prediction {result_df}")

    # Save the result to a CSV (if required)
    result_df.to_csv(predicted_stock_data_file_path, index=False)
    insert_dataframe_postgres(result_df, "stock_annual_predicted_insights")
