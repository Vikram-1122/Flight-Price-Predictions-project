import joblib
import pandas as pd

# Load the trained model
model_filename = 'flight_price_prediction_model.pkl'
pipeline = joblib.load(model_filename)

def predict(features):
    # Convert features to DataFrame
    # Replace column_names with actual names used during training

    column_names = ['duration', 'days_left'  , 'airline', 'source_city', 'departure_time', 'stops', 
                        'arrival_time', 'destination_city', 'class']

    column_names = ['duration', 'days_left', 'airline', 'source_city', 'departure_time', 'stops', 'arrival_time', 'destination_city', 'class']

    features_df = pd.DataFrame([features], columns=column_names)
    prediction = pipeline.predict(features_df)
    return prediction[0]

# Example usage
if __name__ == "__main__":
    # Replace with actual feature values
    example_features = [2.25, 1, "Vistara", "Delhi", "Morning", "zero", 
                        "Afternoon", "Mumbai", "Economy"]
    
    predicted_price = predict(example_features)
    print(f"Predicted flight price: {predicted_price}")

