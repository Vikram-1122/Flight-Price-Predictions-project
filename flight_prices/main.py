from fastapi import FastAPI, Query
from pydantic import BaseModel
import pandas as pd
import joblib
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, Float, DateTime
from datetime import datetime

load_dotenv()
model = joblib.load('models/model.joblib')
preprocessor = joblib.load('models/preprocessor.joblib')

DATABASE_URL = "postgresql+psycopg2://postgres:root@localhost:5432/predictions"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class Prediction(Base):
    __tablename__ = 'predictions'
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    source = Column(String)
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

Base.metadata.create_all(bind=engine)

app = FastAPI()

class FlightData(BaseModel):
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
    source: str = "webapp"

@app.post("/predict")
async def predict(data: FlightData):
    df = pd.DataFrame([data.dict(exclude={'source'})])
    processed_features = preprocessor.transform(df)
    prediction = model.predict(processed_features)
    
    db = SessionLocal()
    new_prediction = Prediction(
        source=data.source,
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
        price=prediction[0]
    )
    db.add(new_prediction)
    db.commit()
    db.close()
    
    return {"price": prediction[0]}

@app.get("/past-predictions")
async def get_past_predictions(start_date: str, end_date: str, source: str = Query("all")):
    db = SessionLocal()
    query = db.query(Prediction)
    if start_date and end_date:
        query = query.filter(Prediction.timestamp >= start_date, Prediction.timestamp <= end_date)
    if source != "all":
        query = query.filter(Prediction.source == source)
    
    results = query.all()
    db.close()
    return [{column.name: getattr(result, column.name) for column in result.__table__.columns} for result in results]  

