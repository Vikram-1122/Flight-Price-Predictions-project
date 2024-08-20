# preprocess.py
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer

def get_preprocessor():
    categorical_cols = ['airline', 'source_city', 'departure_time', 'stops', 
                        'arrival_time', 'destination_city', 'class']
    numerical_cols = ['duration', 'days_left']

    # Preprocessing for numerical data
    numerical_transformer = 'passthrough'

    # Preprocessing for categorical data
    categorical_transformer = OneHotEncoder(handle_unknown='ignore')

    # Bundle preprocessing for numerical and categorical data
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', numerical_transformer, numerical_cols),
            ('cat', categorical_transformer, categorical_cols)
        ])

    return preprocessor
