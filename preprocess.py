from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer

def get_preprocessor():
<<<<<<< HEAD
    # List of categorical and numerical columns
=======
>>>>>>> 8517f8ffbc247d117642f07772990f29938c1687
    categorical_cols = ['airline', 'source_city', 'departure_time', 'stops', 
                        'arrival_time', 'destination_city', 'class']
    numerical_cols = ['duration', 'days_left']

<<<<<<< HEAD
    # Pass-through transformer for numerical data (no transformation applied)
    numerical_transformer = 'passthrough'

    # OneHotEncoder for categorical data
    categorical_transformer = OneHotEncoder(handle_unknown='ignore')

    # ColumnTransformer to apply the transformations
=======
    # Preprocessing for numerical data
    numerical_transformer = 'passthrough'

    # Preprocessing for categorical data
    categorical_transformer = OneHotEncoder(handle_unknown='ignore')

    # Bundle preprocessing for numerical and categorical data
>>>>>>> 8517f8ffbc247d117642f07772990f29938c1687
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', numerical_transformer, numerical_cols),
            ('cat', categorical_transformer, categorical_cols)
        ])

    return preprocessor
<<<<<<< HEAD
    
=======
>>>>>>> 8517f8ffbc247d117642f07772990f29938c1687
