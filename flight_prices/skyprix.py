import streamlit as st
import pandas as pd
import requests
from datetime import datetime

# Define API endpoints
PREDICT_URL = "http://localhost:8000/predict"
PAST_PREDICT_URL = "http://localhost:8000/past-predictions"

# Add background image and styling
def add_background():
    st.markdown(
        """
        <style>
        .stApp {
            background-image: url("https://img.freepik.com/premium-photo/january-vacation-concept-with-airplane-tickets-yellow-background-with-copy-space-top-view_102583-3147.jpg");
            background-size: cover;
            background-position: center;
            background-repeat: no-repeat;
            background-attachment: fixed;
        }
        .stHeader {
            color: #0000FF;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

# Single prediction form
def single_prediction_form():
    st.header("Single Flight Price Prediction")
    
    # Create form
    with st.form("flight_price_form"):
        airline = st.selectbox("Airline", ["AirAsia", "SpiceJet", "Vistara", "GO_FIRST", "Indigo", "Air_India"])
        flight = st.selectbox("Flight Number", ["GB-123", "AA-234", "UK-127", "AI-143"])
        source_city = st.selectbox("Source City", ["Mumbai", "Delhi", "Hyderabad", "Bangalore", "Kolkata", "Chennai"])
        departure_time = st.selectbox("Departure Time", ["Early_Morning", "Morning", "Afternoon", "Evening", "Night"])
        stops = st.selectbox("Stops", ["zero", "one", "two", "three", "four"])
        arrival_time = st.selectbox("Arrival Time", ["Morning", "Early_Morning", "Afternoon", "Evening", "Night"])
        destination_city = st.selectbox("Destination City", ["Delhi", "Hyderabad", "Bangalore", "Kolkata", "Chennai", "Mumbai"])
        travel_class = st.selectbox("Travel Class", ["Economy", "Business", "First"])
        duration = st.number_input("Duration (in hours)", min_value=2.0, step=0.1)
        days_left = st.number_input("Days Left to Departure", min_value=0, step=1)
        price = st.number_input("Price (in your currency)", min_value=2500, step=100)
        
        # Submit button for the form
        predict_button = st.form_submit_button("Predict")

        # When the form is submitted
        if predict_button:
            # Prepare the input data for the API
            data = {
                'data': {
                    'airline': airline,
                    'flight': flight,
                    'source_city': source_city,
                    'departure_time': departure_time,
                    'stops': stops,
                    'arrival_time': arrival_time,
                    'destination_city': destination_city,
                    'class': travel_class,
                    'duration': duration,
                    'days_left': days_left,
                    'price': price
                }
            }

            try:
                # Make API call
                response = requests.post(PREDICT_URL, json=data)
                
                # Check if the response is successful
                if response.status_code == 200:
                    # Display the predicted price
                    predicted_price = response.json().get("prediction", "No prediction available")
                    st.write(f"Predicted Flight Price: **{predicted_price}**")
                else:
                    st.write(f"Error in calling API. Status code: {response.status_code}")
            except Exception as err:
                st.write(f"An error occurred: {err}")

# Multiple prediction form for file upload
def multiple_prediction_form():
    st.header("Multiple Flight Price Prediction")
    
    uploaded_file = st.file_uploader("Upload your CSV file", type=["csv"])
    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
        st.write("Data Preview:", df.head())
        
        if st.button("Predict"):
            data = df.to_dict(orient="records")
            json_data = {"data": data}

            try:
                response = requests.post(PREDICT_URL, json=json_data)
                if response.status_code == 200:
                    predictions = response.json()
                    result_df = pd.DataFrame(predictions, columns=["Predicted Price"])
                    output_df = pd.concat([df, result_df], axis=1)
                    st.write("Predictions:")
                    st.dataframe(output_df)
                else:
                    st.write(f"Error in calling API. Status code: {response.status_code}")
            except Exception as err:
                st.write(f"An error occurred: {err}")

# Past predictions retrieval
def show_past_predictions_page():
    st.header("Past Predictions")
    
    start_date = st.date_input("Start Date", value=datetime(2024, 8, 1))
    end_date = st.date_input("End Date", value=datetime.now())
    prediction_source = st.selectbox("Prediction Source", ["all", "webapp", "scheduled predictions"])
    
    if st.button("Retrieve Past Predictions"):
        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "prediction_source": prediction_source
        }
        
        try:
            response = requests.get(PAST_PREDICT_URL, params=params)
            if response.status_code == 200:
                past_predictions = response.json()
                if past_predictions:
                    st.write("Past Predictions:")
                    st.table(pd.DataFrame(past_predictions))
                else:
                    st.write("No predictions found for the given date range and source.")
            else:
                st.write(f"Error in retrieving predictions. Status code: {response.status_code}")
        except Exception as err:
            st.write(f"An error occurred: {err}")

# Main Streamlit app
def main():
    st.set_page_config(layout="wide")
    st.title("𝗪𝗲𝗹𝗰𝗼𝗺𝗲 𝘁𝗼 𝗦𝗸𝘆𝗽𝗿𝗶𝘅")
    
    add_background()

    # Navigation sidebar
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox("Go to", ["Single Prediction", "Multiple Prediction", "Past Predictions"])

    # Routing to different pages
    if page == "Single Prediction":
        single_prediction_form()
    elif page == "Multiple Prediction":
        multiple_prediction_form()
    elif page == "Past Predictions":
        show_past_predictions_page()

if __name__ == "__main__":
    main()
