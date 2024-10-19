from fastapi import FastAPI, UploadFile, File
from pydantic import BaseModel
import joblib
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, Column, Float, Integer, String, DateTime, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
import io
from typing import List, Union
import uvicorn

app = FastAPI()

# Load your pre-trained model for predictions
model = joblib.load("flight_price_prediction_model.pkl")

# Database connection parameters
pw = "root"  # Replace with your actual database password
DATABASE_URL = "postgresql+psycopg2://postgres:root@localhost:5432/predictions"
engine = create_engine(DATABASE_URL, echo=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Define the prediction record database model
class PredictionRecord(Base):
    __tablename__ = "predictions"

    id = Column(Integer, primary_key=True, nullable=False)
    airline = Column(String)
    flight = Column(String)
    source_city = Column(String)
    departure_time = Column(String)
    stops = Column(String)
    arrival_time = Column(String)
    destination_city = Column(String)
    travel_class = Column(String)
    duration = Column(Float)
    days_left = Column(Integer)
    price = Column(Float)
    predict_date = Column(DateTime, default=datetime.utcnow)
    predict_result = Column(Float)
    predict_source = Column(String(4))

# Define Pydantic models for input validation
class FlightInputData(BaseModel):
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

class FileData(BaseModel):
    file: Union[List[List[Union[str, float, int]]], FlightInputData]
    prediction_source: str

class PastPredictionData(BaseModel):
    start_date: str 
    end_date: str
    prediction_source: str 

# Single flight prediction endpoint
@app.post("/predict")
async def predict(data: FileData):
    # Check if the input is a single flight or multiple flights
    if isinstance(data.file, FlightInputData):
        input_data = [
            data.file.airline,
            data.file.flight,
            data.file.source_city,
            data.file.departure_time,
            data.file.stops,
            data.file.arrival_time,
            data.file.destination_city,
            data.file.travel_class,
            data.file.duration,
            data.file.days_left,
            data.file.price
        ]

        # Make the prediction
        prediction = model.predict([input_data])
        if prediction[0] <= 0:
            prediction[0] = 10000
            
        db = SessionLocal()
        db_prediction = PredictionRecord(
            airline=data.file.airline,
            flight=data.file.flight,
            source_city=data.file.source_city,
            departure_time=data.file.departure_time,
            stops=data.file.stops,
            arrival_time=data.file.arrival_time,
            destination_city=data.file.destination_city,
            travel_class=data.file.travel_class,
            duration=data.file.duration,
            days_left=data.file.days_left,
            price=data.file.price,
            predict_date=datetime.now(),
            predict_result=round(prediction[0], 2),
            predict_source=data.prediction_source,
        )
        db.add(db_prediction)
        db.commit()
        db.refresh(db_prediction)

        return {"predictions": prediction[0], "data": input_data}
    
    elif isinstance(data.file, List):
        # Handle multiple flight predictions
        predictions = model.predict(data.file)
        predictions_list = predictions.tolist()

        # Create a DataFrame for batch processing
        columns = ["airline", "flight", "source_city", "departure_time", "stops",
                   "arrival_time", "destination_city", "travel_class", "duration",
                   "days_left", "price"]
        
        df = pd.DataFrame(data.file, columns=columns)

        df["predict_date"] = datetime.now()
        df["predict_result"] = predictions_list
        df["predict_source"] = data.prediction_source

        data_dict = df.to_dict(orient="records")

        db = SessionLocal()
        db.bulk_insert_mappings(PredictionRecord, data_dict)
        db.commit()

        return {"predictions": predictions_list, "original_data": data_dict}

# Get past predictions endpoint
@app.get("/past-predictions")
async def get_predictions(data: PastPredictionData):
    start_date = f"{data.start_date} 00:00:00"
    end_date = f"{data.end_date} 00:00:00"
    prediction_source = data.prediction_source

    statement = select(
        PredictionRecord.airline,
        PredictionRecord.flight,
        PredictionRecord.source_city,
        PredictionRecord.departure_time,
        PredictionRecord.predict_date,
        PredictionRecord.predict_source,
        PredictionRecord.predict_result
    ).where(
        PredictionRecord.predict_date >= start_date,
        PredictionRecord.predict_date <= end_date
    )
    
    if prediction_source != 'all':
        statement = statement.where(
            PredictionRecord.predict_source == prediction_source)

    db = SessionLocal()
    result = db.execute(statement)

    return result.mappings().all()

# Run the FastAPI application
if __name__ == "__main__":
    uvicorn.run("main_api:app", host="127.0.0.1", port=8000)
