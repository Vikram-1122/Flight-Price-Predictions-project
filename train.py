import sys
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline
import joblib
from preprocess import get_preprocessor

# Load the dataset
sys.path.append('..')
file_path = r'../dsp-skyprix/flight_data.csv'
flight_data = pd.read_csv(file_path)

# Drop unnecessary columns
flight_data_cleaned = flight_data.drop(columns=['Unnamed: 0', 'flight'])

# Define features and target
X = flight_data_cleaned.drop(columns='price')
y = flight_data_cleaned['price']

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Get the preprocessor from preprocess.py
preprocessor = get_preprocessor()

# Define the model
model = LinearRegression()

# Create a pipeline that combines preprocessing with model training
pipeline = Pipeline(steps=[('preprocessor', preprocessor),
                           ('model', model)])

# Train the model
pipeline.fit(X_train, y_train)

# Save the model to a file
model_filename = 'flight_price_prediction_model.pkl'
joblib.dump(pipeline, model_filename)

print(f"Model trained and saved as {model_filename}")
