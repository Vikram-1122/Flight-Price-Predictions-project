import streamlit as st
import requests
import pandas as pd
from datetime import datetime
from io import StringIO

# Endpoint URLs of FastAPI.
PREDICT = 'http://localhost:8000/predict'
PAST_PREDICT_URL = 'http://localhost:8000/past-predictions'

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

def prediction_page():
    st.header("Flight Price Prediction Page")

    st.subheader("Single Flight Prediction")

    # Input fields for flight features
    airline = st.selectbox("Airline", ["AirAsia", "SpiceJet", "Vistara", "GO_FIRST", "Indigo", "Air_India"])
    flight = st.text_input("Flight Number", value = "FN1234")
    source_city = st.selectbox("Source City", ["Mumbai", "Delhi", "Hyderabad", "Bangalore", "Kolkata", "Chennai"])
    departure_time = st.selectbox("Departure Time", ["Early_Morning", "Morning", "Afternoon", "Evening", "Night"])
    stops = st.selectbox("Stops", ["zero", "one", "two", "three", "four"])
    arrival_time = st.selectbox("Arrival Time", [ "Morning","Early_Morning", "Afternoon", "Evening", "Night"])
    destination_city = st.selectbox("Destination City", ["Delhi", "Hyderabad", "Bangalore", "Kolkata", "Chennai","Mumbai"])
    travel_class = st.selectbox("Travel Class", ["Economy", "Business", "First"])
    duration = st.number_input("Duration (in hours)", min_value=2.0, step=0.1)
    days_left = st.number_input("Days Left to Departure", min_value=0, step=1)
    price = st.number_input("Price (in your currency)", min_value=2500, step=100)

    if st.button("Predict"):
        data = {
            'airline': airline,
            'flight': flight,
            'source_city': source_city,
            'departure_time': departure_time,
            'stops': stops,
            'arrival_time': arrival_time,
            'destination_city': destination_city,
            'travel_class': travel_class,
            'duration': duration,
            'days_left': days_left,
            'price': price
        }

        with st.spinner("Making prediction..."):
            try:
                response = requests.post("http://localhost:8000/predict", json=data)
                response.raise_for_status()
                result = response.json()

                input_df = pd.DataFrame([data])
                result_df = pd.DataFrame([result['predictions']])
                
                st.write("Input Values:")
                st.table(input_df)
                st.write("Prediction:")
                st.table(result_df)
            except requests.exceptions.HTTPError as http_err:
                st.write(f"HTTP error occurred: {http_err}")
            except Exception as err:
                st.write(f"An error occurred: {err}")

    st.subheader("Multiple Flight Predictions")
    file = st.file_uploader("Upload CSV file", type=["csv"])

    if file is not None:
        try:
            file_content = file.read().decode('utf-8')
            if not file_content.strip():
                st.write("Uploaded file is empty.")
            else:
                file.seek(0)
                df = pd.read_csv(StringIO(file_content))
                st.write("CSV Content:")
                st.write(df.head())

                st.subheader("Processing file...")
                try:
                    response = requests.post(PREDICT, files={"file": file})
                    response.raise_for_status()
                    result = response.json()
                    predictions = result.get('predictions', [])
                    df['Prediction'] = predictions

                    st.write("Predictions:")
                    st.write(df)
                except requests.exceptions.HTTPError as http_err:
                    st.write(f"HTTP error occurred: {http_err}")
                except Exception as err:
                    st.write(f"An error occurred: {err}")
        except Exception as e:
            st.write(f"An error occurred while processing the file: {e}")

def show_past_predictions_page():
    st.header("Past Predictions Page")
    start_date = st.date_input("Start Date", value=datetime(2024, 8, 1))
    end_date = st.date_input("End Date", value=datetime.now())
    prediction_source = st.selectbox("Prediction Source", ["all", "webapp", "scheduled predictions"])

    if st.button("Retrieve Past Predictions"):
        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "prediction_source": prediction_source
        }

        with st.spinner("Retrieving past predictions..."):
            try:
                response = requests.get(PAST_PREDICT_URL, params=params)
                response.raise_for_status()
                past_predictions = response.json()
                if past_predictions:
                    st.write("Past Predictions:")
                    st.table(pd.DataFrame(past_predictions))
                else:
                    st.write("No predictions found for the given date range and source.")
            except requests.exceptions.HTTPError as http_err:
                st.write(f"HTTP error occurred: {http_err}")
            except Exception as err:
                st.write(f"An error occurred: {err}")


def main():
    st.set_page_config(layout="wide")
    st.title("𝗪𝗲𝗹𝗰𝗼𝗺𝗲 𝘁𝗼 𝗦𝗸𝘆𝗽𝗿𝗶𝘅")

    add_background()
    
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox("Go to", ["Prediction", "Past Predictions"])

    if page == "Prediction":
        prediction_page()
    elif page == "Past Predictions":
        show_past_predictions_page()

if __name__ == "__main__":
    main()
