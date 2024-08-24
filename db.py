from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

# Database configuration
SQLALCHEMY_DATABASE_URL = "postgresql+psycopg2://postgres:root@localhost:5432/predictions"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class Prediction(Base):
    __tablename__ = "predictions"
    id = Column(Integer, primary_key=True, index=True)
    prediction_date = Column(DateTime)
    airline = Column(String)
    flight = Column(String)
    source_city = Column(String)
    departure_time = Column(String)
    stops = Column(String)
    arrival_time = Column(String)
    destination_city = Column(String)
    travel_class = Column(String)  # Updated to match the CSV column name
    duration = Column(Float)
    days_left = Column(Integer)
    price = Column(Float)
    prediction_result = Column(Float)
    prediction_source = Column(String)

def init_db():
    Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
