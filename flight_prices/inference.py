import joblib
import pandas as pd

# Load the trained model
model_filename = 'flight_price_prediction_model.pkl'
pipeline = joblib.load(model_filename)

def predict(features):
    # Convert features to DataFrame

    column_names = ['duration', 'days_left'  , 'airline', 'source_city', 'departure_time', 'stops', 
                        'arrival_time', 'destination_city', 'travel_class']

    if len(features) != len(column_names):
        raise ValueError(f"Expected {len(column_names)} features, but got {len(features)}")

    
    features_df = pd.DataFrame([features], columns=column_names)
    prediction = pipeline.predict(features_df)
    return prediction[0]

# Example usage
if __name__ == "__main__":
    # Replace with actual feature values
    example_features = [2.25, 12, "Vistara", "Delhi", "Morning", "zero", 
                        "Afternoon", "Mumbai", "Economy"]
    
    predicted_price = predict(example_features)
    print(f"Predicted flight price: {predicted_price}")

