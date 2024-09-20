### Flight Price Prediction Application

---

#### Overview
This project involves building an ML-powered flight price prediction application. The primary objective is to predict flight prices based on various features such as airline, flight number, source city, destination city, departure and arrival times, stops, travel class, and other flight details. The prediction system will allow users to input flight data and get price predictions either manually (through JSON) or by uploading a `.csv` file. 

The system also features a continuous flow of data through Airflow, which ingests new data, validates it, and performs scheduled predictions. Data quality is monitored and alerts are sent to Microsoft Teams in case of validation failures or other issues.

#### Architecture

- **User Interface**: A web app built with Streamlit for making on-demand flight price predictions and viewing past predictions.
- **Model Service (API)**: Developed using FastAPI, exposing endpoints to make predictions and retrieve past predictions.
- **Database**: PostgreSQL is used to store past predictions along with the features used for prediction.
- **Prediction Job**: Scheduled using Airflow to periodically make predictions on ingested data from the `good-data` folder.
- **Data Validation**: Using Great Expectations to validate the flight data (e.g., duration, travel class, etc.) before processing.
- **Monitoring Dashboard**: Grafana is used to monitor data quality, prediction performance, and potential data drift.
  
#### Workflow

**User-end**:
- Users can manually input flight details via a JSON format or upload a `.csv` file containing multiple flight records.
- The web application calls the model API for predictions and displays the predicted prices.
- Users can also view past predictions based on different filters.

**Prediction-end**:
- Airflow's ingestion pipeline continuously monitors the `raw-data` folder for new files.
- Once a file is ingested, the data is validated for quality issues (e.g., missing values, incorrect data types, etc.).
- Files with no errors are moved to the `good-data` folder, while problematic files are moved to the `bad-data` folder.
- Alerts are sent to Microsoft Teams for any validation issues encountered.
- A prediction job runs every 5 minutes, making predictions on the newly ingested files in the `good-data` folder.

---

#### Dataset Description
The dataset used for predictions contains the following features:

- `airline`: Name of the airline (e.g., SpiceJet, AirAsia, Vistara)
- `flight`: Flight number (e.g., SG-8709, I5-764)
- `source_city`: City of departure (e.g., Delhi)
- `departure_time`: Departure time of the flight (Morning, Evening, etc.)
- `stops`: Number of stops (e.g., zero, one)
- `arrival_time`: Arrival time of the flight
- `destination_city`: City of arrival (e.g., Mumbai)
- `travel_class`: Travel class (Economy, Business)
- `duration`: Flight duration in hours
- `days_left`: Number of days left before the flight
- `price`: Price of the flight (Target variable)

---

#### API Endpoints
The model service API exposes the following endpoints:
- **POST /predict**: Makes predictions based on flight data provided in JSON format as well as Allows users to upload a `.csv` file for bulk predictions.
- **GET /past-predictions**: Retrieves past predictions along with the input features used.

---

#### Airflow DAGs

**Data Ingestion DAG**:
- `read-data`: Reads a random file from the `raw-data` folder.
- `validate-data`: Validates the data using Great Expectations to ensure data quality.
- `save-statistics`: Saves statistics (e.g., number of rows, valid/invalid rows) to the database.
- `send-alerts`: Generates a report on data validation results and sends alerts via Microsoft Teams.
- `split-and-save-data`: Splits the data into `good-data` and `bad-data` folders based on validation results.

**Prediction DAG**:
- `check_for_new_data`: Checks for new files in the `good-data` folder.
- `make_predictions`: Reads the new data files and makes predictions by calling the model API.

---

#### Installation

1. **Install Dependencies**:
   ```
   pip install -r requirements.txt
   ```

2. **Run Streamlit Application**:
   ```
   streamlit run skyprix.py
   ```

3. **Set Up Airflow** (using Docker):
   - Build and start Airflow with Docker:
     ```
     docker-compose up airflow-init
     docker-compose up
     ```
   - Access the Airflow web UI:
     ```
     http://localhost:8080
     ```

4. **Great Expectations**:
   Install Great Expectations for data validation:
   ```
   pip install great_expectations
   ```



---

#### Usage

**Prediction Webpage**:
- Use the Streamlit web interface to input flight details for single or multiple predictions.
- Predictions can be made manually (through JSON input) or by uploading a `.csv` file.

**Past Predictions Webpage**:
- View past predictions by selecting the date range and prediction source (`webapp`, `scheduled predictions`, or `all`).


---

This README provides an overview of the Flight Price Prediction Application's architecture, setup, and workflow. For more details, consult individual component documentation and code comments.
