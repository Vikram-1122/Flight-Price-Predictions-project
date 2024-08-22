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
