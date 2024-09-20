from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.compose import ColumnTransformer

def get_preprocessor():
    categorical_cols = ['airline', 'source_city', 'departure_time', 'stops', 
                        'arrival_time', 'destination_city', 'travel_class']
    numerical_cols = ['duration', 'days_left']

    # Preprocess the numerical columns with StandardScaler
    numerical_transformer = StandardScaler()

    # Preprocess the categorical columns with OneHotEncoder
    categorical_transformer = OneHotEncoder(handle_unknown='ignore')

    # Column transformation of both numerical and categorical columns
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', numerical_transformer, numerical_cols),
            ('cat', categorical_transformer, categorical_cols)
        ])

    return preprocessor
