import joblib
import streamlit as st
import pandas as pd
<<<<<<< Updated upstream

# Default values for inputs
default_flight_number = "AI2023"
default_duration = 2.0  # Example default duration in hours
default_days_left = 10   # Example default days left
default_price = 5000     # Example default price in currency

st.title("Streamlit Interface for Flight Price Prediction")

def page1():
    # Input fields for single flight prediction
    airline = st.selectbox("Airline", ["AirAsia", "SpiceJet", "Vistara", "GO_FIRST", "Indigo", "Air_India"], index=0)
    flight = st.text_input("Flight Number", value=default_flight_number)
    source_city = st.selectbox("Source City", ["Mumbai", "Delhi", "Hyderabad", "Bangalore", "Kolkata", "Chennai"], index=0)
    departure_time = st.selectbox("Departure Time", ["Early_Morning", "Morning", "Afternoon", "Evening", "Night"], index=1)
    stops = st.selectbox("Stops", ["zero", "one", "two", "three", "four"], index=0)
    arrival_time = st.selectbox("Arrival Time", ["Morning", "Early_Morning", "Afternoon", "Evening", "Night"], index=2)
    destination_city = st.selectbox("Destination City", ["Delhi", "Hyderabad", "Bangalore", "Kolkata", "Chennai", "Mumbai"], index=0)
    travel_class = st.selectbox("Travel Class", ["Economy", "Business", "First"], index=0)
    duration = st.number_input("Duration (in hours)", min_value=2.0, step=0.1, value=default_duration)
    days_left = st.number_input("Days Left to Departure", min_value=0, step=1, value=default_days_left)
    price = st.number_input("Price (in your currency)", min_value=2500, step=100, value=default_price)

    st.write("---")
    return {
        "type": "single",
        "data": {
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
    }

def page2():
    # File uploader for multiple flight predictions
    uploaded_file = st.file_uploader("Upload your CSV file", type=["csv"])
    if uploaded_file is not None:
        st.write("---")
        return {
            "type": "multiple",
            "data": uploaded_file
        }

def main():
    st.write("---")
    pages = ["From Inputs", "From CSV"]
    selected_page = st.selectbox("Select your prediction type", pages)
    st.write("----")
    
    if selected_page == "From Inputs":
        prediction_data = page1()
    elif selected_page == "From CSV":
        prediction_data = page2()
    
    if st.button("Predict"):
        if prediction_data["type"] == "single":
            response = requests.post("http://localhost:8000/predict", json={"file": prediction_data["data"], "prediction_source": "web"})
        elif prediction_data["type"] == "multiple":
            csv_content = prediction_data["data"].read().decode("utf-8")
            rows = [row.split(',') for row in csv_content.split('\n') if row.strip()]
            data = {
                "file": rows,
                "prediction_source": "web"
            }
            response = requests.post("http://localhost:8000/predict", json=data)
        
        if response.status_code == 200:
            prediction = response.json()
            if prediction_data["type"] == "single":
                if "predictions" in prediction and "data" in prediction:
                    st.subheader("The estimated price of the flight is:")
                    st.write(round(prediction["predictions"], 2))
                    st.subheader("Based on the data received below:")
                    
                    single_predict_data = pd.DataFrame({
                        "Airline": prediction_data["data"]["airline"],
                        "Flight": prediction_data["data"]["flight"],
                        "Source City": prediction_data["data"]["source_city"],
                        "Departure Time": prediction_data["data"]["departure_time"],
                        "Stops": prediction_data["data"]["stops"],
                        "Arrival Time": prediction_data["data"]["arrival_time"],
                        "Destination City": prediction_data["data"]["destination_city"],
                        "Travel Class": prediction_data["data"]["travel_class"],
                        "Duration": prediction_data["data"]["duration"],
                        "Days Left": prediction_data["data"]["days_left"],
                        "Current Price": prediction_data["data"]["price"],
                        "Estimated Price": round(prediction["predictions"], 2)
                    }, index=[1])

                    st.write(single_predict_data)

            elif prediction_data["type"] == "multiple":
                if "original_data" in prediction and "predictions" in prediction:
                    st.write("Original Data:")
                    df_result = pd.DataFrame(prediction["original_data"])
                    st.write(df_result)
        else:
            st.write("An error occurred during prediction.")
=======
import requests

st.title('Flight Price Prediction')
st.header('Single Prediction')

preprocessor = joblib.load('models/preprocessor.joblib')
model = joblib.load('models/model.joblib')

airline = st.selectbox("Airline", ["AirAsia", "SpiceJet", "Vistara", "GO_FIRST", "Indigo", "Air_India"])
flight = st.text_input("Flight Number")
source_city = st.selectbox("Source City", ["Mumbai", "Delhi", "Hyderabad", "Bangalore", "Kolkata", "Chennai"])
departure_time = st.selectbox("Departure Time", ["Early_Morning", "Morning", "Afternoon", "Evening", "Night"])
stops = st.selectbox("Stops", ["zero", "one", "two", "three", "four"])
arrival_time = st.selectbox("Arrival Time", ["Morning", "Early_Morning", "Afternoon", "Evening", "Night"])
destination_city = st.selectbox("Destination City", ["Delhi", "Hyderabad", "Bangalore", "Kolkata", "Chennai", "Mumbai"])
travel_class = st.selectbox("Travel Class", ["Economy", "Business", "First"])
duration = st.number_input("Duration (in hours)", min_value=2.0, step=0.1)
days_left = st.number_input("Days Left to Departure", min_value=0, step=1)

if st.button('Predict'):
    input_data = {
    "airline": "Air_India",
    "flight": "AI-431",
    "source_city": "Delhi",
    "departure_time": "Afternoon",
    "stops": "one",
    "arrival_time": "Afternoon",
    "destination_city": "Mumbai",
    "travel_class": "Economy",
    "duration": 24.5,
    "days_left": 31,
    "price": 5231
}
    
    response = requests.post('http://localhost:8000/predict', json=input_data)
    st.write('Predicted Price:', response.json()['price'])

st.header('Bulk Prediction')
uploaded_file = st.file_uploader('Upload a CSV file', type='csv')
if uploaded_file:
    input_df = pd.read_csv(uploaded_file)
    response = requests.post('http://localhost:8000/predict', json=input_df.to_dict(orient='records'))
    st.write('Predicted Prices:', response.json())
>>>>>>> Stashed changes

st.header('Past Predictions')
date_range = st.date_input('Select Date Range', [])
prediction_source = st.selectbox('Select Prediction Source', ['webapp', 'scheduled predictions', 'all'])

if st.button('Fetch Past Predictions'):
    params = {'start_date': date_range[0], 'end_date': date_range[1], 'source': prediction_source}
    response = requests.get('http://localhost:8000/past-predictions', params=params)
    predictions = pd.DataFrame(response.json())
    st.write(predictions)
