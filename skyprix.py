import streamlit as st
import requests
import pandas as pd
from datetime import datetime
from io import StringIO

# FastAPI endpoint URLs
PREDICT_URL = 'http://localhost:8000/predict/'
PAST_PREDICTIONS_URL = 'http://localhost:8000/past-predictions'

def add_background():
    st.markdown(
        """
        <style>
        .stApp {
            background-image: url("https://wallpapers.com/images/featured/4k-plane-39efmsuz0fhub1c3.jpg");
            background-size: cover;
            background-position: center;
            background-repeat: no-repeat;
            background-attachment: fixed;
        }
        .stHeader {
            color: #ffffff;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

# Prediction page
def prediction_page():
    st.header("Flight Price Prediction Page")

    st.subheader("Single Flight Prediction")

    # Input fields for flight features
    airline = st.selectbox("Airline", ["AirAsia", "SpiceJet", "Vistara", "GO_FIRST", "Indigo", "Air_India"])
    flight = st.text_input("Flight Number")
    source_city = st.selectbox("Source City", ["Mumbai", "Delhi", "Hyderabad", "Bangalore", "Kolkata", "Chennai"])
    departure_time = st.selectbox("Departure Time", ["Early_Morning", "Morning", "Afternoon", "Evening", "Night"])
    stops = st.selectbox("Stops", ["zero", "one", "two", "three", "four"])
    arrival_time = st.selectbox("Arrival Time", ["Early_Morning", "Morning", "Afternoon", "Evening", "Night"])
    destination_city = st.selectbox("Destination City", ["Mumbai", "Delhi", "Hyderabad", "Bangalore", "Kolkata", "Chennai"])
    travel_class = st.selectbox("Travel Class", ["Economy", "Business", "First"])  # Updated to 'travel_class'
    duration = st.number_input("Duration (in hours)", min_value=0.0, step=0.1)
    days_left = st.number_input("Days Left to Departure", min_value=0, step=1)
    price = st.number_input("Price (in your currency)", min_value=0.0, step=100.0)

    if st.button("Predict"):
        data = {
            'airline': airline,
            'flight': flight,
            'source_city': source_city,
            'departure_time': departure_time,
            'stops': stops,
            'arrival_time': arrival_time,
            'destination_city': destination_city,
            'travel_class': travel_class,  # Updated to 'travel_class'
            'duration': duration,
            'days_left': days_left,
            'price': price
        }

        response = requests.post(PREDICT_URL, json={"input": data})
        if response.status_code == 200:
            result = response.json()
            table_data = {**data, **result}
            st.write("Prediction:")
            st.table(pd.DataFrame([table_data]))
        else:
            st.write(f"Error making prediction: {response.text}")

    st.subheader("Multiple Flight Predictions")
    file = st.file_uploader("Upload CSV file", type=["csv"])

    if file:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(file)

        # Remove the first column if it's a serial number
        if df.columns[0].startswith('Unnamed'):
            df = df.iloc[:, 1:]

        # Define required columns
        required_columns = {
            'airline', 'flight', 'source_city', 'departure_time', 'stops', 
            'arrival_time', 'destination_city', 'travel_class', 'duration', 
            'days_left', 'price'
        }

        # Check for missing columns
        missing_columns = required_columns - set(df.columns)
        if missing_columns:
            st.write(f"Error: CSV file is missing the following columns: {', '.join(missing_columns)}")
            return

        # Check for extra columns
        extra_columns = set(df.columns) - required_columns
        if extra_columns:
            st.write(f"Note: CSV file contains extra columns that will be ignored: {', '.join(extra_columns)}")

        # Prepare data for prediction
        data_for_prediction = df[list(required_columns)]  # Ensure only required columns are included
        data_for_prediction = data_for_prediction.to_dict(orient='records')

        # Make API request with the CSV data
        response = requests.post(PREDICT_URL, json={"file": data_for_prediction})
        if response.status_code == 200:
            result = response.json()
            predictions = result.get('predictions', [])
            df['Prediction'] = predictions
            st.write("Predictions:")
            st.table(df)
        else:
            st.write(f"Error making prediction: {response.text}")

# Past predictions display webpage
def past_predictions_page():
    st.header("Past Flight Predictions Display Webpage")
    
    start_date = st.date_input("Start Date", datetime(2024, 1, 1))
    end_date = st.date_input("End Date", datetime(2024, 12, 31))
    prediction_source = st.selectbox("Prediction Source", ["file_upload", "json_input", "all"])

    if st.button("Retrieve Predictions"):
        params = {
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'prediction_source': prediction_source
        }
        response = requests.get(PAST_PREDICTIONS_URL, params=params)
        if response.status_code == 200:
            past_predictions = response.json()
            if past_predictions:
                st.write("Past Predictions:")
                st.table(pd.DataFrame(past_predictions))
            else:
                st.write("No past predictions found.")
        else:
            st.write(f"Error retrieving past predictions: {response.text}")

# Main Streamlit app
def main():
    st.set_page_config(layout="wide")
    st.title("𝗪𝗲𝗹𝗰𝗼𝗺𝗲 𝘁𝗼 𝗦𝗸𝘆𝗽𝗿𝗶𝘅")

    add_background()
    
    page = st.sidebar.radio("Select Page", ["Prediction", "Past Predictions"])

    if page == "Prediction":
        prediction_page()
    elif page == "Past Predictions":
        past_predictions_page()

if __name__ == "__main__":
    main()
