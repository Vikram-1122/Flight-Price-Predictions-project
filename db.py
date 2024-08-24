from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# PostgreSQL database URL
SQLALCHEMY_DATABASE_URL = "postgresql://postgres:root@localhost:5432/predictions"

# Create the engine
engine = create_engine(SQLALCHEMY_DATABASE_URL, echo=True)  # Set echo=True for SQL query logging (optional)

# Create a configured "Session" class
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create a base class for declarative class definitions
Base = declarative_base()

class Prediction(Base):
    __tablename__ = "predictions"

    id = Column(Integer, primary_key=True, index=True)
    prediction_date = Column(Date, default=datetime.utcnow)
    airline = Column(String, index=True)
    flight = Column(String)
    source_city = Column(String)
    departure_time = Column(Time)
    stops = Column(Integer)
    arrival_time = Column(Time)
    destination_city = Column(String)
    class_flight = Column("class", String)  
    duration = Column(Float)
    days_left = Column(Integer)
    price = Column(Float)
    prediction_result = Column(Float)
    prediction_source = Column(String)

# Function to initialize the database
def init_db():
    """Create the tables in the database."""
    Base.metadata.create_all(bind=engine)

# Example usage
if __name__ == "__main__":
    init_db()
