from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from inference import predict

app = FastAPI()

class PredictionRequest(BaseModel):
    features: list

@app.post("/predict")
async def predict_price(request: PredictionRequest):
    features = request.features
    if len(features) != 9:
        raise HTTPException(status_code=400, detail="Invalid number of features")
    
    prediction = predict(features)
    return {"predicted_price": prediction}
