import random
import numpy as np
import pandas as pd
import sys
sys.path.append('..')
# Load dataset (replace with your actual file path)
data = pd.read_csv('./testing.csv')

# Function to introduce errors into the dataset
def introduce_errors_randomly(df, num_errors=15):
    error_types = [
        'missing_column', 
        'missing_value', 
        'unknown_value', 
        'outlier', 
        'type_mismatch', 
        'incorrect_value', 
        'shuffle', 
        'inconsistent', 
        'duplicate_row', 
        'irrelevant_value', 
        'zero_value', 
        'extreme_value', 
        'string_in_numeric', 
        'negative_value', 
        'invalid_date_format'
    ]
    
    # Get a random sample of row indices where errors will be introduced
    error_indices = random.sample(range(len(df)), num_errors)
    
    for idx, error_type in zip(error_indices, error_types):
        col = random.choice(df.columns)
        
        if error_type == 'missing_column':
            # Drop a random column entirely
            col_to_drop = random.choice(df.columns)
            df.drop(col_to_drop, axis=1, inplace=True)
        
        elif error_type == 'missing_value':
            # Introduce missing values in random columns
            df.loc[idx, col] = np.nan
        
        elif error_type == 'unknown_value':
            # Insert an unknown value in a column with expected values
            df.loc[idx, col] = 'Unknown_Value'
        
        elif error_type == 'outlier':
            # Introduce extreme outlier in numeric columns
            if df[col].dtype in ['int64', 'float64']:
                df.loc[idx, col] = df[col].max() * 100
        
        elif error_type == 'type_mismatch':
            # Introduce a type mismatch in numeric columns (string in numeric)
            if df[col].dtype in ['int64', 'float64']:
                df.loc[idx, col] = 'invalid_data'
        
        elif error_type == 'incorrect_value':
            # Introduce incorrect values (e.g., negative age or invalid category)
            if df[col].dtype in ['int64', 'float64']:
                df.loc[idx, col] = -999  # Invalid negative value
        
        elif error_type == 'shuffle':
            # Shuffle a random column
            df[col] = df[col].sample(frac=1).values
        
        elif error_type == 'inconsistent':
            # Introduce inconsistent values (e.g., mismatched categories)
            df.loc[idx, col] = 'Inconsistent_Value'
        
        elif error_type == 'duplicate_row':
            # Introduce a duplicate row
            duplicate_row = df.iloc[[idx]].copy()
            df = pd.concat([df, duplicate_row], ignore_index=True)
        
        elif error_type == 'irrelevant_value':
            # Add irrelevant values in a column
            df.loc[idx, col] = 'Irrelevant_Value'
        
        elif error_type == 'zero_value':
            # Set a numerical column to zero
            if df[col].dtype in ['int64', 'float64']:
                df.loc[idx, col] = 0
        
        elif error_type == 'extreme_value':
            # Insert extreme large or small values
            if df[col].dtype in ['int64', 'float64']:
                df.loc[idx, col] = 1e10  # Example extreme value
        
        elif error_type == 'string_in_numeric':
            # Insert a string into a numeric column
            if df[col].dtype in ['int64', 'float64']:
                df.loc[idx, col] = 'string_value'
        
        elif error_type == 'negative_value':
            # Insert a negative value in a column where it's not valid (e.g., age, price)
            if df[col].dtype in ['int64', 'float64']:
                df.loc[idx, col] = -random.randint(1, 100)
        
        elif error_type == 'invalid_date_format':
            # Insert an invalid date in a date column
            df.loc[idx, col] = '32/13/2023'  # Invalid date format

    return df

# Introduce errors randomly into the dataset
data_with_errors = introduce_errors_randomly(data.copy(), num_errors=15)

# Save the dataset with errors
data_with_errors.to_csv('error_dataset.csv', index=False)

print("Errors introduced successfully! Check the 'error_dataset.csv' file.")