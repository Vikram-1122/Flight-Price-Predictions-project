<<<<<<< HEAD
import streamlit as st
import requests
import pandas as pd
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

=======
import requests
import streamlit as st
import pandas as pd

# FastAPI endpoint URLs
PREDICT_URL = 'http://localhost:8000/predict-flight'
PAST_PREDICTIONS_URL = 'http://localhost:8000/past-flight-predictions'
>>>>>>> 8517f8ffbc247d117642f07772990f29938c1687

# Prediction page
def prediction_page():
    st.header("Flight Price Prediction Page")

    # Display form for single sample prediction
    st.subheader("Single Flight Prediction")
    
    # Input fields for flight features
    airline = st.selectbox("Airline", ["AirAsia", "SpiceJet", "Vistara", "GO_FIRST", "Indigo", "Air_India"])
    flight = st.text_input("Flight Number")
    source_city = st.selectbox("Source City", ["Mumbai", "Delhi", "Hyderabad", "Bangalore", "Kolkata", "Chennai"])
    departure_time = st.selectbox("Departure Time", ["Early_Morning", "Morning", "Afternoon", "Evening", "Night"])
    stops = st.selectbox("Stops", ["zero", "one", "two", "three", "four"])
    arrival_time = st.selectbox("Arrival Time", ["Early_Morning", "Morning", "Afternoon", "Evening", "Night"])
    destination_city = st.selectbox("Destination City", ["Mumbai", "Delhi", "Hyderabad", "Bangalore", "Kolkata", "Chennai"])
<<<<<<< HEAD
    class_flight = st.selectbox("Flight Class", ["Economy", "Business", "First"])
    duration = st.number_input("Duration (in hours)", min_value=0.0, step=0.1)
    days_left = st.number_input("Days Left to Departure", min_value=0, step=1)
    price = st.number_input("Price (in your currency)", min_value=0.0, step=100.0)
=======
    class_ = st.selectbox("Class", ["Economy", "Business", "First"])
    duration = st.number_input("Duration (in hours)", min_value=0.0, step=0.1)
    days_left = st.number_input("Days Left to Departure", min_value=0, step=1)
>>>>>>> 8517f8ffbc247d117642f07772990f29938c1687
    
    # Predict button
    if st.button("Predict"):
        data = {
            'airline': airline,
            'flight': flight,
            'source_city': source_city,
            'departure_time': departure_time,
            'stops': stops,
            'arrival_time': arrival_time,
            'destination_city': destination_city,
<<<<<<< HEAD
            'class': class_flight,
            'duration': duration,
            'days_left': days_left,
            'price': price
=======
            'class': class_,
            'duration': duration,
            'days_left': days_left
>>>>>>> 8517f8ffbc247d117642f07772990f29938c1687
        }
        
        # Make API request to model service with feature values
        response = requests.post(PREDICT_URL, json=data)
        if response.status_code == 200:
            result = response.json()
            table_data = {**data, **result}
            table_data = {k: [v] for k, v in table_data.items()}
            # Display prediction result
            st.write("Prediction:")
            st.table(table_data)
        else:
            st.write("Error making prediction:", response.text)

    # Upload CSV file for multiple predictions
    st.subheader("Multiple Flight Predictions")
    file = st.file_uploader("Upload CSV file", type=["csv"])

<<<<<<< HEAD
    if file and st.button("Predict Multiple"):
        # Convert file to string and then to pandas DataFrame
        file_contents = file.read().decode('utf-8')
        df = pd.read_csv(StringIO(file_contents))
        
        # Check if 'price' column exists and drop the serial number column
        if 'price' in df.columns:
            df_cleaned = df.drop(columns=df.columns[0])  # Drop the serial number column
        else:
            st.write("Error: CSV file must contain a 'price' column.")
            return
        
        data = df_cleaned.to_dict('records')

        # Make API request to model service with feature values and display prediction results
        response = requests.post(PREDICT_URL, files={'file': ('data_1.csv', StringIO(file_contents), 'text/csv')})
        if response.status_code == 200:
            result = response.json()
            predictions = result['predictions']
            predictions_data = df_cleaned.copy()
            predictions_data['Prediction'] = predictions
            st.write("Predictions:")
            st.table(predictions_data)
        else:
            st.write("Error making prediction:", response.text)
=======
    if file is not None and st.button("Predict Multiple"):
        # Read CSV file and extract feature values
        df = pd.read_csv(file)
        data = df.to_dict('records')

        # Make API request to model service with feature values and display prediction results
        predictions = []
        for row in data:
            response = requests.post(PREDICT_URL, json=row)
            if response.status_code == 200:
                result = response.json()
                row['Prediction'] = result['prediction']
                predictions.append(row)
            else:
                st.write("Error making prediction:", response.text)

        # Display prediction results
        if predictions:
            st.write("Predictions:")
            st.table(predictions)
        else:
            st.write("No predictions to display.")
>>>>>>> 8517f8ffbc247d117642f07772990f29938c1687

# Past predictions display webpage
def past_predictions_page():
    st.header("Past Flight Predictions Display Webpage")
    
    # Date selection component
    start_date = st.date_input("Start Date")
    end_date = st.date_input("End Date")
    
    # Prediction source drop-down list
    prediction_source = st.selectbox("Prediction Source", ["webapp", "scheduled predictions", "all"])
    
    if st.button("Retrieve Predictions"):
        # Make API request to model service with date range and prediction source
        params = {
            'start_date': start_date,
            'end_date': end_date,
            'prediction_source': prediction_source
        }
        response = requests.get(PAST_PREDICTIONS_URL, params=params)
        if response.status_code == 200:
            # Get past predictions from model service
            past_predictions = response.json()
            # Display past predictions
            if past_predictions:
                st.write("Prediction Date:")
                past_predictions_df = pd.DataFrame(past_predictions)
                st.table(past_predictions_df)
            else:
                st.write("No past predictions found.")
        else:
            st.write("Error retrieving past predictions:", response.text)

# Main Streamlit app
def main():
    st.set_page_config(layout="wide")
<<<<<<< HEAD
    st.title("𝗪𝗲𝗹𝗰𝗼𝗺𝗲 𝘁𝗼 𝗦𝗸𝘆𝗽𝗿𝗶𝘅")

    # Add background
    add_background()
=======
    st.title("𝗪𝗲𝗹𝗰𝗼𝗺𝗲 𝘁𝗼 𝗦𝗸𝘆𝗣𝗿𝗶𝘅")
>>>>>>> 8517f8ffbc247d117642f07772990f29938c1687
    
    # Navigation menu
    page = st.sidebar.radio("Select Page", ["Prediction", "Past Predictions"])

    # Display selected page
    if page == "Prediction":
        prediction_page()
    elif page == "Past Predictions":
        past_predictions_page()

if __name__ == "__main__":
<<<<<<< HEAD
    main()
=======
    main()
>>>>>>> 8517f8ffbc247d117642f07772990f29938c1687
