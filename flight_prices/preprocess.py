from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer

def get_preprocessor():
    categorical_cols = ['airline', 'source_city', 'departure_time', 'stops', 
                        'arrival_time', 'destination_city', 'travel_class']
    numerical_cols = ['duration', 'days_left']

    # preprocess the numerical columns.
    numerical_transformer = 'passthrough'

    # Preprocess the categorical columns.
    categorical_transformer = OneHotEncoder(handle_unknown='ignore')

    # column transformation of both numerical and categorical columns.
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', numerical_transformer, numerical_cols),
            ('cat', categorical_transformer, categorical_cols)
        ])

    return preprocessor
