from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer

def get_preprocessor():
    # List of categorical and numerical columns
    categorical_cols = ['airline', 'source_city', 'departure_time', 'stops', 
                        'arrival_time', 'destination_city', 'class']
    numerical_cols = ['duration', 'days_left']

    # Pass-through transformer for numerical data (no transformation applied)
    numerical_transformer = 'passthrough'

    # OneHotEncoder for categorical data
    categorical_transformer = OneHotEncoder(handle_unknown='ignore')

    # ColumnTransformer to apply the transformations
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', numerical_transformer, numerical_cols),
            ('cat', categorical_transformer, categorical_cols)
        ])

    return preprocessor
    