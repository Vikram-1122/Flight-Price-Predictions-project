
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

# Load the pre-trained model (assuming it's stored as a .pkl file)
model = joblib.load("flight_price_prediction_model.pkl")

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

# Utility functions to handle file and JSON input
def handle_file(file: UploadFile) -> pd.DataFrame:
    file_content = file.file.read().decode('utf-8')
    if not file_content.strip():
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")
    df = pd.read_csv(StringIO(file_content))
    return df

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

    # Ensure either file or JSON input is provided
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
            logger.error(f"JSON input error: {str(e)}")
            raise HTTPException(status_code=400, detail="Error processing JSON input.")
    else:
        raise HTTPException(status_code=400, detail="No input provided.")

    # Predict using the pre-loaded model
    try:
        predictions = model.predict(df)
        df["predictions"] = predictions
    except Exception as e:
        logger.error(f"Prediction error: {str(e)}")
        raise HTTPException(status_code=500, detail="Prediction failed.")

    # Save predictions and input features to the database (simplified example)
    for _, row in df.iterrows():
        prediction = Prediction(
            airline=row["airline"],
            flight=row["flight"],
            source_city=row["source_city"],
            departure_time=row["departure_time"],
            stops=row["stops"],
            arrival_time=row["arrival_time"],
            destination_city=row["destination_city"],
            travel_class=row["travel_class"],
            duration=row["duration"],
            days_left=row["days_left"],
            price=row["price"],
            prediction=row["predictions"],
            timestamp=datetime.now()
        )
        db.add(prediction)
        db.commit()

    return {"predictions": df.to_dict(orient="records")}

@app.get("/past-predictions")
async def past_predictions(
    start_date: datetime,
    end_date: datetime,
    source: str = "all",
    db: Session = Depends(get_db)
):
    query = db.query(Prediction).filter(
        Prediction.timestamp.between(start_date, end_date)
    )
    
    if source == "webapp":
        query = query.filter(Prediction.source == "webapp")
    elif source == "scheduled":
        query = query.filter(Prediction.source == "scheduled")

    results = query.all()
    return {"predictions": [r.as_dict() for r in results]}
