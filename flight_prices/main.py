from typing import Union
from fastapi import FastAPI, UploadFile, File, HTTPException, Body, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session
import pandas as pd
import joblib
from io import StringIO
from datetime import datetime
from db import Prediction, get_db
import logging


app = FastAPI()
logger = logging.getLogger("uvicorn")
logging.basicConfig(level=logging.INFO)


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
    price: int

# to handle file input
def handle_file(file: UploadFile) -> pd.DataFrame:
    file_content = file.file.read().decode('utf-8')
    if not file_content.strip():
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")
    df = pd.read_csv(StringIO(file_content))
    return df

# to handle json input
def handle_json(input: SinglePredictionInput) -> pd.DataFrame:
    df = pd.DataFrame([input.dict()])
    return df

@app.post("/predict")
async def predict(
    file: UploadFile = File(None),
    input: Union[SinglePredictionInput, None] = Body(None),
    db: Session = Depends(get_db)
):
    logger.info(f"Received file: {file.filename if file else 'None'}")
    logger.info(f"Received JSON input: {input if input else 'None'}")

    
    if file and input:
        raise HTTPException(status_code=400, detail="Provide either a file or JSON input, not both.")
    
    if file:
        try:
            df = handle_file(file)
        except Exception as e:
            logger.error(f"File processing error: {str(e)}")
            raise HTTPException(status_code=400, detail="Error processing the uploaded file.")
    elif input:
        try:
            df = handle_json(input)
        except Exception as e:
            logger.error(f"JSON processing error: {str(e)}")
            raise HTTPException(status_code=400, detail="Error processing JSON input.")
    else:
        raise HTTPException(status_code=400, detail="No file or JSON input provided.")

    # Load the model
    pipeline_filename = 'flight_price_prediction_model.pkl'
    try:
        pipeline = joblib.load(pipeline_filename)
    except FileNotFoundError:
        logger.error("Prediction model not found.")
        raise HTTPException(status_code=500, detail="Prediction model not available.")

    
    predictions = pipeline.predict(df)
    
    
    return {"predictions": predictions.tolist()}

 

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

if __name__ == "__main__":
    main()
