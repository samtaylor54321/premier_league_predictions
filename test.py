import requests
import pandas as pd

url = "http://localhost:5000/results"

data = pd.read_csv("/opt/airflow/data/current_gameweek.csv")

for i in range(data.shape[0]):
    r = requests.post(url, json=data.iloc[i, 1:15].to_dict())

print(r.json())
