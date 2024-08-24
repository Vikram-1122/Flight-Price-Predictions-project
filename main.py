from fastapi import FastAPI, UploadFile, File, HTTPException, Body, Depends
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

# Initialize the database
init_db()

# Pydantic model for single JSON input
class SinglePredictionInput(BaseModel):
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
    price: float

def handle_file(file: UploadFile):
    contents = file.file.read().decode('utf-8')
    df = pd.read_csv(StringIO(contents))
    logger.info(f"File loaded successfully with {df.shape[0]} rows and {df.shape[1]} columns.")
    
    required_columns = ['airline', 'flight', 'source_city', 'departure_time', 'stops', 'arrival_time', 'destination_city', 'travel_class', 'duration', 'days_left', 'price']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.error(f"Missing columns: {', '.join(missing_columns)}")
        raise HTTPException(status_code=400, detail=f"Missing columns: {', '.join(missing_columns)}")

    if df.shape[1] > len(required_columns):
        df_cleaned = df.iloc[:, 1:]
    else:
        df_cleaned = df

    if 'price' not in df_cleaned.columns:
        logger.error("Target column 'price' is missing.")
        raise HTTPException(status_code=400, detail="Target column 'price' is missing.")
    
    X = df_cleaned.drop(columns='price')
    return X

def handle_json(input: SinglePredictionInput):
    X = pd.DataFrame([input.dict()])
    logger.info(f"Single prediction input received: {input.dict()}")
    return X

@app.post("/predict/")
async def predict_price(
    file: UploadFile = File(None),
    input: SinglePredictionInput = Body(None),
    db: Session = Depends(get_db)
):
    if file is None and input is None:
        raise HTTPException(status_code=400, detail="Either a file or JSON input is required.")

    if file:
        X = handle_file(file)
    elif input:
        X = handle_json(input)
    else:
        raise HTTPException(status_code=400, detail="Either a file or JSON input is required.")

    pipeline_filename = 'flight_price_prediction_model.pkl'
    try:
        pipeline = joblib.load(pipeline_filename)
    except FileNotFoundError:
        logger.error("Pipeline file not found.")
        raise HTTPException(status_code=500, detail="Pipeline file not found")
    
    try:
        predictions = pipeline.predict(X).tolist()
    except Exception as e:
        logger.error(f"Error in preprocessing or prediction: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Error in preprocessing or prediction: {str(e)}")
    
    for i in range(len(predictions)):
        features = X.iloc[i].to_dict()
        prediction = predictions[i]
        db_prediction = Prediction(
            prediction_date=datetime.now().date(),
            airline=features.get('airline', ''),
            flight=features.get('flight', ''),
            source_city=features.get('source_city', ''),
            departure_time=features.get('departure_time', ''),
            stops=features.get('stops', ''),
            arrival_time=features.get('arrival_time', ''),
            destination_city=features.get('destination_city', ''),
            travel_class=features.get('travel_class', ''),
            duration=features.get('duration', 0.0),
            days_left=features.get('days_left', 0),
            price=features.get('price', 0.0),
            prediction_result=prediction,
            prediction_source='file_upload' if file else 'json_input'
        )
        db.add(db_prediction)
    
    db.commit()
    
    return {"predictions": predictions}

@app.get("/past-predictions")
def get_past_predictions(
    start_date: datetime,
    end_date: datetime,
    prediction_source: str = "all",
    db: Session = Depends(get_db)
):
    try:
        query = db.query(Prediction).filter(
            Prediction.prediction_date >= start_date.date(),
            Prediction.prediction_date <= end_date.date()
        )
        
        if prediction_source != "all":
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
                "prediction_result": pred.prediction_result,
                "prediction_source": pred.prediction_source
            }
            for pred in past_predictions
        ]
        
        return results
    
    except Exception as e:
        logger.error(f"An error occurred while retrieving past predictions: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred while retrieving past predictions: {str(e)}")
