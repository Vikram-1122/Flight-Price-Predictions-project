import great_expectations as ge
from great_expectations.data_context.data_context import DataContext

# Change this path to where you want to initialize Great Expectations
project_directory = "C:/Users/sumith singh/dsp-skyprix/airflow-alpha/great_expectations"

# Create a new Great Expectations project
context = DataContext.create(project_directory)

print(f"Great Expectations project initialized at: {project_directory}")