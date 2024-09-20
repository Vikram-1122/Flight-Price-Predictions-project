import os
import glob
import sys
sys.path.append('..')

def remove_csv_files(folder_path):
    csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
    
    for file in csv_files:
        os.remove(file)
        print(f"Removed: {file}")


folder_path = './airflow-alpha/raw_data' 
remove_csv_files(folder_path)
