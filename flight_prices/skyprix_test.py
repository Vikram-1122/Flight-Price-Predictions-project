import streamlit as st
import pandas as pd
import requests
from datetime import datetime

PREDICT_URL = "http://localhost:8000/predict"
PAST_PREDICT_URL = "http://localhost:8000/past-predictions"

def prediction_page():
    st.header("Prediction Page")
    
    # Single Prediction Form
    with st.form("single_prediction_form"):
        st.subheader("Single Prediction")
        airline = st.text_input("Airline")
        flight = st.text_input("Flight")
        source_city = st.text_input("Source City")
        departure_time = st.text_input("Departure Time")
        stops = st.text_input("Stops")
        arrival_time = st.text_input("Arrival Time")
        destination_city = st.text_input("Destination City")
        travel_class = st.selectbox("Travel Class", ["Economy", "Business"])
        duration = st.number_input("Duration", min_value=2.0)
        days_left = st.number_input("Days Left", min_value=0)
        price = st.number_input("Price", min_value=2500)
        
        submit_single = st.form_submit_button("Predict")
        
        if submit_single:
            payload = {
                "airline": airline,
                "flight": flight,
                "source_city": source_city,
                "departure_time": departure_time,
                "stops": stops,
                "arrival_time": arrival_time,
                "destination_city": destination_city,
                "travel_class": travel_class,
                "duration": duration,
                "days_left": days_left,
                "price": price
            }
            
            with st.spinner("Making prediction..."):
                try:
                    response = requests.post(PREDICT_URL, json=payload)
                    response.raise_for_status()
                    prediction_result = response.json()
                    st.write(prediction_result)
                except Exception as e:
                    st.write(f"An error occurred: {e}")
    
    # Multiple Predictions (CSV Upload)
    st.subheader("Multiple Predictions")
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file)
            st.write(df.head())
            
            with st.spinner("Making multiple predictions..."):
                try:
                    files = {"file": uploaded_file}
                    response = requests.post(PREDICT_URL, files=files)
                    response.raise_for_status()
                    multi_prediction_result = response.json()
                    st.write(multi_prediction_result)
                except Exception as e:
                    st.write(f"An error occurred while processing the file: {e}")
        except Exception as e:
            st.write(f"An error occurred while reading the file: {e}")

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
            except Exception as e:
                st.write(f"An error occurred: {e}")

def main():
    st.set_page_config(layout="wide")
    st.title("Welcome to Skyprix")

    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox("Go to", ["Prediction", "Past Predictions"])

    if page == "Prediction":
        prediction_page()
    elif page == "Past Predictions":
        show_past_predictions_page()

if __name__ == "__main__":
    main()
