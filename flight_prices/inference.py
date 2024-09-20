import joblib
import pandas as pd
import numpy as np

# Load the saved preprocessor (scaler and encoder) and model
scaler_filename = 'models/scaler.joblib'
encoder_filename = 'models/encoder.joblib'
model_filename = 'models/model.joblib'

scaler = joblib.load(scaler_filename)
encoder = joblib.load(encoder_filename)
model = joblib.load(model_filename)

def preprocess_and_predict(features):
    """
    Preprocess the input features and make a prediction using the saved model.
    """
    column_names = ['duration', 'days_left', 'airline', 'source_city', 'departure_time', 'stops', 
                    'arrival_time', 'destination_city', 'travel_class']

    if len(features) != len(column_names):
        raise ValueError(f"Expected {len(column_names)} features, but got {len(features)}")

    # Convert input features to a DataFrame
    features_df = pd.DataFrame([features], columns=column_names)

    # Separate numerical and categorical features for preprocessing
    numerical_cols = ['duration', 'days_left']
    categorical_cols = ['airline', 'source_city', 'departure_time', 'stops', 
                        'arrival_time', 'destination_city', 'travel_class']

    # Scale numerical features
    scaled_numerical = scaler.transform(features_df[numerical_cols])

    # Encode categorical features
    encoded_categorical = encoder.transform(features_df[categorical_cols])

    # Combine the scaled numerical and encoded categorical features
    processed_features = np.hstack([scaled_numerical, encoded_categorical.toarray()])

    # Make a prediction using the saved model
    prediction = model.predict(processed_features)
    return prediction[0]

# Example usage
if __name__ == "__main__":
    # Here you can replace them with your own feature values.
    example_features = [10, 30, "Vistara", "Delhi", "Morning", "One", 
                        "Afternoon", "Mumbai", "Economy"]
    
    predicted_price = preprocess_and_predict(example_features)
    print(f"Predicted flight price: {predicted_price}")
