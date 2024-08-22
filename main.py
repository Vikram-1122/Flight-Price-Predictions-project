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
