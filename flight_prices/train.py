import sys
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
import joblib
from preprocess import get_preprocessor

# Load the dataset
sys.path.append('..')
file_path = r'../flight_data.csv'
flight_data = pd.read_csv(file_path)

# Drop unnecessary columns
flight_data_cleaned = flight_data.drop(columns=['Unnamed: 0', 'flight'])

# Define features and target
X = flight_data_cleaned.drop(columns='price')  # Features
y = flight_data_cleaned['price']  # Target (flight price)

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42)

# Get the preprocessor from preprocess.py
preprocessor = get_preprocessor()

# Fit the preprocessor on the training data
X_train_preprocessed = preprocessor.fit_transform(X_train)

# Define and train the model
model = LinearRegression()
model.fit(X_train_preprocessed, y_train)

# Save the preprocessor and model separately
scaler = preprocessor.named_transformers_['num']
encoder = preprocessor.named_transformers_['cat']

joblib.dump(scaler, '../flight_prices/models/scaler.joblib')
joblib.dump(encoder, '../flight_prices/models/encoder.joblib')
joblib.dump(model, '../flight_prices/models/model.joblib')

print("Scaler, encoder, and model saved successfully.")
