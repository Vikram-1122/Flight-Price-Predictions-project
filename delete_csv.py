import os
import glob
import sys
sys.path.append('..')
# Function to remove all CSV files in the folder
def remove_csv_files(folder_path):
    csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
    
    for file in csv_files:
        os.remove(file)
        print(f"Removed: {file}")

# Example usage
folder_path = './airflow-alpha/raw_data'  # Replace with your folder path
remove_csv_files(folder_path)
