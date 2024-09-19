import pandas as pd
import numpy as np
import sys
sys.path.append('..')

# Load the dataset
file_path = ('./testing.csv' )
df = pd.read_csv(file_path)

# Strip whitespace from column names
df.columns = df.columns.str.strip()

# Dictionary to track errors
error_log = {
    'missing_column': [],
    'missing_values': [],
    'unknown_values': [],
    'wrong_values': [],
    'string_in_numeric': [],
    'duplicated_rows': 0,  # Track number of duplicated rows
    'incorrect_data_type': [],
    'duration_errors': []  # New error log for duration errors
}

# 1. Add unknown values for 'class' and 'airline'
unknown_class_sample = df.sample(frac=0.01).index
df.loc[unknown_class_sample, 'travel_class'] = 'Premium'  # Invalid value
error_log['unknown_values'].extend([(idx, 'travel_class') for idx in unknown_class_sample])

unknown_airline_sample = df.sample(frac=0.01).index
df.loc[unknown_airline_sample, 'airline'] = 'AirFrance'  # Invalid value
error_log['unknown_values'].extend([(idx, 'airline') for idx in unknown_airline_sample])

# 2. Add wrong values for 'price'
wrong_value_sample = df.sample(frac=0.01).index
wrong_values = [-1000, 0, -5, 9999999]
df.loc[wrong_value_sample, 'price'] = np.random.choice(wrong_values, size=len(wrong_value_sample))
error_log['wrong_values'].extend([(idx, 'price') for idx in wrong_value_sample])

# 3. Add string values to 'price' (should be numeric)
string_in_numeric_sample = df.sample(frac=0.01).index
df['price'] = df['price'].astype('object')  # Convert 'price' to object type temporarily
df.loc[string_in_numeric_sample, 'price'] = 'not available'
error_log['string_in_numeric'].extend([(idx, 'price') for idx in string_in_numeric_sample])

# Convert 'price' to numeric, forcing errors to NaN
df['price'] = pd.to_numeric(df['price'], errors='coerce')

# 4. Duplicated rows
duplicated_sample = df.sample(frac=0.001).copy()  
df = pd.concat([df, duplicated_sample])
error_log['duplicated_rows'] += len(duplicated_sample)

# 5. Incorrect data type: Insert numerical values into 'class'
incorrect_type_sample = df.sample(frac=0.01).index
df.loc[incorrect_type_sample, 'travel_class'] = 12345
error_log['incorrect_data_type'].extend([(idx, 'class') for idx in incorrect_type_sample])

# 6. Introduce missing column (e.g., 'days_left')
required_column = 'days_left'
if required_column in df.columns:
    df = df.drop(columns=[required_column])
    error_log['missing_column'].append(required_column)

# 7. Introduce missing values in 'price'
missing_value_sample = df.sample(frac=0.01).index
df.loc[missing_value_sample, 'price'] = np.nan
error_log['missing_values'].extend([(idx, 'price') for idx in missing_value_sample])

# 8. Introduce 'Paris' as an error in 'source_city'
missing_source_city_sample = df.sample(frac=0.01).index
df.loc[missing_source_city_sample, 'source_city'] = 'Paris'
error_log['unknown_values'].extend([(idx, 'source_city') for idx in missing_source_city_sample])

# 9. Introduce duration errors
if 'duration' in df.columns:
    duration_error_sample = df.sample(frac=0.01).index  # 1% of duration errors
    invalid_durations = ['-1h 30m', '1000h', '25:61', 'invalid', '23hours']  # Various types of invalid durations
    
    # Assign invalid durations one by one to ensure matching size
    for i, idx in enumerate(duration_error_sample):
        df.loc[idx, 'duration'] = np.random.choice(invalid_durations)
        error_log['duration_errors'].append((idx, 'duration'))

# Ensure consistent data types in the DataFrame before exporting
df = df.convert_dtypes()

# Print out the error log summary
print("\nError Log Summary:")
for error_type, details in error_log.items():
    if error_type == 'duplicated_rows':
        print(f"{error_type}: {details} duplicated rows added")
    else:
        print(f"{error_type}: {len(details)} errors added")
        print(f"Sample errors: {details[:5]}")  # Display the first 5 entries of each type of error

# Save the dataset with errors to the current directory
output_path =  './error_dataset.csv'
df.to_csv(output_path, index=False)
print(f"\nDataset with errors saved to {output_path}")