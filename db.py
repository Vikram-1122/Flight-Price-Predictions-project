
from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

Base = declarative_base()

class Prediction(Base):
    __tablename__ = 'predictions'

    id = Column(Integer, primary_key=True, index=True)
    prediction_date = Column(DateTime)
    airline = Column(String)
    flight = Column(String)
    source_city = Column(String)
    departure_time = Column(DateTime)  # or Time if you prefer
    stops = Column(Integer)
    arrival_time = Column(DateTime)  # or Time if you prefer
    destination_city = Column(String)
    class_flight = Column(String)
    duration = Column(Float)
    days_left = Column(Integer)
    price = Column(Float)
    prediction_result = Column(Float)
    prediction_source = Column(String)

# Set up the database engine and session
engine = create_engine('sqlite:///./test.db')  # Replace with your database URL
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_db():
    Base.metadata.create_all(bind=engine)
