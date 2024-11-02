import great_expectations as ge
from great_expectations.data_context import FileDataContext

# Set the project directory where you want to initialize Great Expectations
project_directory = "C:/Users/sumith singh/dsp-skyprix/airflow-alpha/great_expectations"

# Create a new Great Expectations project using FileDataContext
context = FileDataContext(project_directory)

print(f"Great Expectations project initialized at: {project_directory}")
