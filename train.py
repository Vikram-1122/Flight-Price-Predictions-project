import joblib
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline
from preprocess import get_preprocessor
from sklearn.model_selection import train_test_split

# Load the dataset
file_path = r'../dsp-skyprix/flight_data.csv'
flight_data = pd.read_csv(file_path)

# Drop unnecessary columns
flight_data_cleaned = flight_data.drop(columns=['Unnamed: 0', 'flight'])

# Define features and target
X = flight_data_cleaned.drop(columns='price')
y = flight_data_cleaned['price']

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42)

# Get the preprocessor
preprocessor = get_preprocessor()

# Define the model
model = LinearRegression()

# Create a pipeline that combines preprocessing with model training
pipeline = Pipeline(steps=[('preprocessor', preprocessor),
                           ('model', model)])

# Train the model
pipeline.fit(X_train, y_train)

# Save the pipeline to a file
joblib.dump(pipeline, 'flight_price_prediction_pipeline.pkl')

print("Model and preprocessor trained and saved.")

