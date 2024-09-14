from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

# configure the database using your server_name, username, password , host and  database_name.
SQLALCHEMY_DATABASE_URL = "postgresql+psycopg2://postgres:root@localhost:5432/predictions"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# define a class with the following required columns.
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
    travel_class = Column(String)
    duration = Column(Float)
    days_left = Column(Integer)
    price = Column(Integer, nullable=True)
    prediction_result = Column(Float)
    prediction_source = Column(String)

# intialize the database
def init_db():
    Base.metadata.create_all(bind=engine)

# call the database
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
