import great_expectations as ge

# Specify the path to your Great Expectations project
context = ge.data_context.DataContext("C:/Users/sumith singh/dsp-skyprix/airflow-alpha/great_expectations")

# Optionally, you can perform further actions, like running validations or checking data
print("Great Expectations context loaded successfully!")

import os
print("Current working directory:", os.getcwd())
