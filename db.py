# db.py
from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Database configuration
SQLALCHEMY_DATABASE_URL = "postgresql+psycopg2://postgres:root@localhost:5432/predictions"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class Prediction(Base):
    __tablename__ = "predictions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    prediction_date = Column(Date, nullable=False) 
    airline = Column(String(50), nullable=False)
    flight = Column(String(20))
    source_city = Column(String(50), nullable=False)
    departure_time = Column(String(50), nullable=False)
    stops = Column(String(50), nullable=False)
    arrival_time = Column(String(50), nullable=False)
    destination_city = Column(String(50), nullable=False)
    class_flight = Column(String(10), nullable=False)
    duration = Column(Float, nullable=False)
    days_left = Column(Integer, nullable=False)
    price = Column(Float, nullable=False)
    prediction_result = Column(Float, nullable=False)
    prediction_source = Column(String(50), nullable=False)
def init_db():
    Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
