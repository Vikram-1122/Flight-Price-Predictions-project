import joblib
import pandas as pd

preprocessor = joblib.load('models/preprocessor.joblib')
model = joblib.load('models/model.joblib')

def preprocess_and_predict(features):
    column_names = [
        'duration', 'days_left', 'airline', 'source_city', 
        'departure_time', 'stops', 'arrival_time', 
        'destination_city', 'travel_class'
    ]
    features_df = pd.DataFrame([features], columns=column_names)
    processed_features = preprocessor.transform(features_df)
    return model.predict(processed_features)[0]

if __name__ == "__main__":
    example_features = [
        10, 
        30, 
        "Vistara", 
        "Delhi", 
        "Morning", 
        "One", 
        "Afternoon", 
        "Mumbai", 
        "Economy"
    ]
    predicted_price = preprocess_and_predict(example_features)
    print(f"Predicted flight price: {predicted_price}")
