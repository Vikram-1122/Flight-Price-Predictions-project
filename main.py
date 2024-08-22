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
