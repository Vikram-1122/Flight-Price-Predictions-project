<<<<<<< HEAD
# main.py
from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, Body, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session
import pandas as pd
import joblib
from io import StringIO
from datetime import datetime
import logging
from db import Prediction, get_db, init_db

app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize the database (ensure it's done only once)
init_db()

class SinglePredictionInput(BaseModel):
    airline: str
    flight: str
    source_city: str
    departure_time: str
    stops: str
    arrival_time: str
    destination_city: str
    class_flight: str
    duration: float
    days_left: int
    price: float

@app.post("/predict/")
async def predict_price(
    file: UploadFile = File(None),  # File is optional here
    input: SinglePredictionInput = Body(None),  # Input via JSON body is also optional
    db: Session = Depends(get_db)
):
    if file is None and input is None:
        raise HTTPException(status_code=400, detail="Either a file or JSON input is required.")
    
    try:
        if file:
            # Handling CSV file input
            contents = await file.read()
            df = pd.read_csv(StringIO(contents.decode('utf-8')))
            logger.info(f"File loaded successfully with {df.shape[0]} rows and {df.shape[1]} columns.")

            # Check for required columns
            required_columns = ['airline', 'flight', 'source_city', 'departure_time', 'stops', 'arrival_time', 'destination_city', 'class', 'duration', 'days_left', 'price']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.error(f"Missing columns: {', '.join(missing_columns)}")
                raise HTTPException(status_code=400, detail=f"Missing columns: {', '.join(missing_columns)}")

            # Drop the serial number column if present (assumed to be the first column)
            if df.shape[1] > len(required_columns):
                df_cleaned = df.iloc[:, 1:]  # Drop the first column
            else:
                df_cleaned = df

            # Separate features and target
            if 'price' not in df_cleaned.columns:
                logger.error("Target column 'price' is missing.")
                raise HTTPException(status_code=400, detail="Target column 'price' is missing.")
            X = df_cleaned.drop(columns='price')

        elif input:
            # Handling single JSON input
            # Convert the input to a DataFrame
            X = pd.DataFrame([input.dict()])  # Convert the input to a DataFrame
            logger.info(f"Single prediction input received: {input.dict()}")

        else:
            raise HTTPException(status_code=400, detail="Either a file or JSON input is required.")
        
        # Load the pipeline (which includes the fitted preprocessor and model)
        pipeline_filename = 'flight_price_prediction_pipeline.pkl'
        try:
            pipeline = joblib.load(pipeline_filename)
        except FileNotFoundError:
            logger.error("Pipeline file not found.")
            raise HTTPException(status_code=500, detail="Pipeline file not found")
        
        # Preprocess and make predictions
        try:
            predictions = pipeline.predict(X).tolist()
        except Exception as e:
            logger.error(f"Error in preprocessing or prediction: {str(e)}")
            raise HTTPException(status_code=400, detail=f"Error in preprocessing or prediction: {str(e)}")
        
        # Save predictions and features to the database
        for i in range(len(predictions)):
            features = X.iloc[i].to_dict()
            prediction = predictions[i]
            db_prediction = Prediction(
                prediction_date=datetime.now(),
                airline=features.get('airline', ''),
                flight=features.get('flight', ''),
                source_city=features.get('source_city', ''),
                departure_time=features.get('departure_time', ''),  # Store as string
                stops=features.get('stops', 0),
                arrival_time=features.get('arrival_time', ''),      # Store as string
                destination_city=features.get('destination_city', ''),
                class_flight=features.get('class_flight', ''),
                duration=features.get('duration', 0.0),
                days_left=features.get('days_left', 0),
                price=features.get('price', 0.0),
                prediction_result=prediction,
                prediction_source='file_upload' if file else 'json_input'
            )
            db.add(db_prediction)
        
        db.commit()
        
        return {"predictions": predictions}
    
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@app.get("/past-predictions")
def get_past_predictions(
    start_date: datetime = Query(..., description="Start date for the query"),
    end_date: datetime = Query(..., description="End date for the query"),
    prediction_source: str = Query("all", description="Source of the predictions to filter by (webapp, scheduled predictions, all)"),
    db: Session = Depends(get_db)
):
    try:
        # Build query based on parameters
        query = db.query(Prediction).filter(
            Prediction.prediction_date >= start_date,
            Prediction.prediction_date <= end_date
        )
        
        if prediction_source != "all":
            query = query.filter(Prediction.prediction_source == prediction_source)
        
        past_predictions = query.all()
        
        # Convert to a list of dictionaries
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
                "class_flight": pred.class_flight,
                "duration": pred.duration,
                "days_left": pred.days_left,
                "price": pred.price,
                "prediction_result": pred.prediction_result,
                "prediction_source": pred.prediction_source
            }
            for pred in past_predictions
        ]
        
        return results
    
    except Exception as e:
        logger.error(f"An error occurred while retrieving past predictions: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred while retrieving past predictions: {str(e)}")
=======
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
from inference import predict  # Ensure this imports correctly

app = FastAPI()

class PredictionRequest(BaseModel):
    features: List[float]  # Specify the type of the elements in the list

@app.post("/predict")
async def predict_price(request: PredictionRequest):
    features = request.features
    if len(features) != 9:
        raise HTTPException(status_code=400, detail="Invalid number of features. Expected 9 features.")

    try:
        prediction = predict(features)  # Ensure predict function handles the input correctly
        return {"predicted_price": prediction}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@app.get("/")
def read_root():
    return {"message": "Welcome to the prediction API. Use the /predict endpoint to get predictions."}
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Dict
import pickle
import json

from db import SessionLocal, Prediction
from preprocess import get_preprocessor  # assuming this is your preprocessing function

app = FastAPI()

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Load your trained model
with open("flight_price_prediction_model.pkl", "rb") as f:
    model = pickle.load(f)

@app.post("/predict/")
def predict(data: List[Dict], db: Session = Depends(get_db)):
    # Preprocess the data
    processed_data = get_preprocessor(data)
    
    # Make predictions
    predictions = model.predict(processed_data).tolist()
    
    # Save predictions and features to the database
    for i in range(len(predictions)):
        features = json.dumps(data[i])
        prediction = predictions[i]
        db_prediction = Prediction(features=features, prediction=prediction)
        db.add(db_prediction)
    
    db.commit()
    
    return {"predictions": predictions}

@app.get("/past-predictions/")
def past_predictions(db: Session = Depends(get_db)):
    predictions = db.query(Prediction).all()
    return [{"features": json.loads(p.features), "prediction": p.prediction, "created_at": p.created_at} for p in predictions]
739c97372a9def29a438d4895e95735b3669ff75
>>>>>>> 8517f8ffbc247d117642f07772990f29938c1687
