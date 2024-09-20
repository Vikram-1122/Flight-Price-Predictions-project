from datetime import datetime
import logging
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Union, Optional
import joblib
import pandas as pd
import uvicorn
from db import Prediction, get_db
from sqlalchemy.orm import Session
import numpy as np

# Initialize FastAPI app
app = FastAPI()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define Pydantic models
class FlightPricePrediction(BaseModel):
    airline: str
    flight: str
    source_city: str
    departure_time: str
    stops: str
    arrival_time: str
    destination_city: str
    travel_class: str
    duration: float
    days_left: int
    price: int

class PredictionRequest(BaseModel):
    data: Union[FlightPricePrediction, List[FlightPricePrediction]]

# Load pre-trained models
try:
    le = joblib.load('models/encoder.joblib')  # Load label encoder
    scaler = joblib.load('models/scaler.joblib')  # Load scaler
    model = joblib.load('models/model.joblib')  # Load model
except FileNotFoundError:
    logger.error("Model or preprocessing files not found. Please check the paths.")
    raise HTTPException(status_code=500, detail="Model not available")

# Function to preprocess and make predictions
def preprocess_and_predict(df: pd.DataFrame) -> List[float]:
    try:
        logger.info(f"Input shape: {df.shape}")

        # Separate numerical and categorical features
        numerical_features = df[['duration', 'days_left', 'price']]
        categorical_features = df[['airline', 'source_city', 'departure_time', 'stops', 
                                    'arrival_time', 'destination_city', 'travel_class']]

        # Scale numerical features
        scaled_numerical = scaler.transform(numerical_features)

        # Encode categorical features
        encoded_categorical = le.transform(categorical_features)

        # Combine processed features
        processed_features = np.hstack([scaled_numerical, encoded_categorical])

        # Make predictions
        predictions = model.predict(processed_features).tolist()
        logger.info(f"Predictions: {predictions}")
        return predictions
    except Exception as e:
        logger.error(f"Error during prediction: {e}")
        raise HTTPException(status_code=500, detail="Prediction failed")

# Function to insert prediction data into the database
def insert_data(db: Session, data: FlightPricePrediction, result: float, source: str):
    table_data = Prediction(
        airline=data.airline,
        flight=data.flight,
        source_city=data.source_city,
        departure_time=data.departure_time,
        stops=data.stops,
        arrival_time=data.arrival_time,
        destination_city=data.destination_city,
        travel_class=data.travel_class,
        duration=data.duration,
        days_left=data.days_left,
        price=data.price,
        predicted_price=result,
        source=source,
    )
    db.add(table_data)
    db.commit()

# Prediction endpoint
@app.post('/predict')
def predict(request: PredictionRequest, db: Session = Depends(get_db)):
    if isinstance(request.data, list):
        dict_data = [row.dict() for row in request.data]
        input_data = pd.DataFrame(dict_data)
        predictions = preprocess_and_predict(input_data)

        for i in range(len(request.data)):
            insert_data(db, request.data[i], predictions[i], 'webapp')
        return predictions
    else:
        dict_data = request.data.dict()
        input_data = pd.DataFrame([dict_data])
        predictions = preprocess_and_predict(input_data)
        insert_data(db, request.data, predictions[0], 'webapp')
        return predictions[0]

@app.get('/past-predictions')
def get_past_predictions(start_date: datetime, end_date: datetime, prediction_source: Optional[str] = None, db: Session = Depends(get_db)):
    try:
        query = db.query(Prediction).filter(
            Prediction.prediction_date >= start_date,
            Prediction.prediction_date <= end_date
        )
        if prediction_source and prediction_source != "all":
            query = query.filter(Prediction.prediction_source == prediction_source)

        past_predictions = query.all()
        
        results = [
            {
                "prediction_date": pred.prediction_date,
                "airline": pred.airline,
                "flight": pred.flight,
                "source_city": pred.source_city,
                "departure_time": pred.departure_time,
                "stops": pred.stops,
                "arrival_time": pred.arrival_time,
                "destination_city": pred.destination_city,
                "travel_class": pred.travel_class,
                "duration": pred.duration,
                "days_left": pred.days_left,
                "price": pred.price,
                "predicted_price": pred.predicted_price,
                "prediction_source": pred.prediction_source
            }
            for pred in past_predictions
        ]
        
        return results
    
    except Exception as e:
        logger.error(f"An error occurred while retrieving past predictions: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred while retrieving past predictions: {str(e)}")

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
