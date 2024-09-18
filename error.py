import pandas as pd
import numpy as np
import random
import sys

sys.path.append('..')

# Function to introduce specific real-world errors into the dataset and track them in an Error_Log column
def introduce_specific_errors(df, num_errors=20):  # Increase the number of errors to 20
    # Error scenarios we want to introduce
    error_scenarios = [
        'missing_value_entity',     # Missing value in a specific entity (e.g., one value from 'Price')
        'missing_value',            # Missing values in a required column
        'unknown_value',            # Unknown values for a given feature (e.g., France in Country column)
        'wrong_value',              # Wrong value for a feature (e.g., negative Age)
        'string_in_numeric',        # A string value in a numerical column
        'duplicate_row',            # Duplicate row introduced
        'incorrect_data_type',      # Incorrect data type: numerical values in categorical columns
        'outlier_value',            # Outlier or extreme values for numerical columns
        'invalid_date_format',      # Invalid date in a date column
        'wrong_column_type'         # Numeric value inserted in categorical column
    ]
    
    # Log of introduced errors
    error_log = []
    
    # Add a new Error_Log column to track errors row by row
    df['Error_Log'] = np.nan
    
    # Get a random sample of row indices where errors will be introduced
    error_indices = random.sample(range(len(df)), num_errors)
    
    for idx, error_type in zip(error_indices, error_scenarios * (num_errors // len(error_scenarios) + 1)):
        
        # Select a random column based on the error type
        if error_type in ['missing_value_entity', 'missing_value', 'unknown_value', 'wrong_value', 'string_in_numeric', 'incorrect_data_type', 'outlier_value', 'invalid_date_format', 'wrong_column_type']:
            col = random.choice(df.columns)
        
        if error_type == 'missing_value_entity':
            # Introduce missing value in a specific entity (e.g., one price value is removed)
            if col == 'Price' and df[col].dtype in ['int64', 'float64']:  # Ensure it's the 'Price' column
                df.loc[idx, col] = np.nan
                df.loc[idx, 'Error_Log'] = f"Missing value in entity 'Price'"
                error_log.append(f"Error: Missing value in entity 'Price' at row {idx}")
        
        elif error_type == 'missing_value':
            # Introduce missing values in a required column
            df.loc[idx, col] = np.nan
            df.loc[idx, 'Error_Log'] = f"Missing value in required column '{col}'"
            error_log.append(f"Error: Missing value in required column '{col}' at row {idx}")
        
        elif error_type == 'unknown_value':
            # Add an unknown value 'France' for a given feature (e.g., Country column)
            if df[col].dtype == 'object' and 'Country' in col:
                df.loc[idx, col] = 'France'
                df.loc[idx, 'Error_Log'] = f"Unexpected value 'France' in column '{col}'"
                error_log.append(f"Error: Unexpected value 'France' in column '{col}' at row {idx}")
        
        elif error_type == 'wrong_value':
            # Add wrong value for a feature (e.g., negative Age)
            if df[col].dtype in ['int64', 'float64']:
                df.loc[idx, col] = -999  # Invalid negative value
                df.loc[idx, 'Error_Log'] = f"Wrong value '-999' in column '{col}'"
                error_log.append(f"Error: Wrong value '-999' in column '{col}' at row {idx}")
        
        elif error_type == 'string_in_numeric':
            # Add string values in a numerical column (e.g., 'invalid_data' in numeric column)
            if df[col].dtype in ['int64', 'float64']:
                df.loc[idx, col] = 'invalid_data'
                df.loc[idx, 'Error_Log'] = f"String 'invalid_data' in numeric column '{col}'"
                error_log.append(f"Error: String 'invalid_data' in numeric column '{col}' at row {idx}")
        
        elif error_type == 'duplicate_row':
            # Duplicate a row in the dataset
            duplicate_row = df.iloc[[idx]].copy()
            duplicate_row['Error_Log'] = f"Duplicated row from index {idx}"
            df = pd.concat([df, duplicate_row], ignore_index=True)
            error_log.append(f"Error: Duplicated row at index {idx}")
        
        elif error_type == 'incorrect_data_type':
            # Insert numerical values into a categorical column (e.g., 'Category' column)
            if df[col].dtype == 'object':
                df.loc[idx, col] = random.randint(1, 100)
                df.loc[idx, 'Error_Log'] = f"Inserted numeric value '{df.loc[idx, col]}' into categorical column '{col}'"
                error_log.append(f"Error: Incorrect data type: inserted numeric value '{df.loc[idx, col]}' into categorical column '{col}' at row {idx}")
        
        elif error_type == 'outlier_value':
            # Add an extreme outlier value (e.g., extremely high or low value)
            if df[col].dtype in ['int64', 'float64']:
                outlier = df[col].mean() * 1000  # Introducing an extreme value (large outlier)
                df.loc[idx, col] = outlier
                df.loc[idx, 'Error_Log'] = f"Outlier value '{outlier}' in column '{col}'"
                error_log.append(f"Error: Outlier value '{outlier}' in column '{col}' at row {idx}")
        
        elif error_type == 'invalid_date_format':
            # Insert an invalid date in a date column
            if 'date' in col.lower():
                df.loc[idx, col] = '32/13/2023'  # Invalid date format
                df.loc[idx, 'Error_Log'] = f"Invalid date '32/13/2023' in column '{col}'"
                error_log.append(f"Error: Invalid date '32/13/2023' in column '{col}' at row {idx}")

        elif error_type == 'wrong_column_type':
            # Add a numeric value to a text/categorical column
            if df[col].dtype == 'object':
                df.loc[idx, col] = 12345  # Insert a numeric value
                df.loc[idx, 'Error_Log'] = f"Numeric value '12345' added to text/categorical column '{col}'"
                error_log.append(f"Error: Numeric value '12345' added to text/categorical column '{col}' at row {idx}")
    
    return df, error_log

# Load your dataset
file_path = './testing.csv'  # Change file path as needed

try:
    data = pd.read_csv(file_path)
    data["Country"] = "India"  # Adding default values for 'Country' column
    print("Dataset loaded successfully!")
except Exception as e:
    print(f"Error loading dataset: {e}")

# Check the size of the dataset and adjust sampling accordingly
try:
    num_rows = len(data)
    print(f"Dataset contains {num_rows} rows.")
    
    if num_rows < 100:
        print("Dataset has fewer than 100 rows. Sampling all rows.")
        sampled_data = data.copy()  # Use the entire dataset if there are fewer than 100 rows
    else:
        sampled_data = data.sample(n=100, random_state=42)
        print("Successfully sampled 100 rows!")
        
except Exception as e:
    print(f"Error sampling data: {e}")

# Introduce errors into the dataset and log errors
try:
    data_with_errors, error_log = introduce_specific_errors(sampled_data.copy(), num_errors=20)  # Increased error count to 20
    
    # Print error log summary
    print("\nError Log Summary:")
    for log in error_log:
        print(log)
    
    # Save the dataset with errors
    output_file = 'error_dataset.csv'
    data_with_errors.to_csv(output_file, index=False)
    print(f"\nDataset with errors saved successfully as '{output_file}'")
    
    # Check the Error_Log column for errors
    print("\nSample of the dataset with errors:")
    print(data_with_errors[['Error_Log']].dropna().head())  # Show rows where errors were logged
    
except Exception as e:
    print(f"Error introducing errors or saving the dataset: {e}")