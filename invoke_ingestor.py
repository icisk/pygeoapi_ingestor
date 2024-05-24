import requests
import json
from datetime import datetime

# ingestor id
ingestor_process = 'ingestor-process'

# parameters for the ingestor
data_dir = "seasonal_forecast"
issue_date = datetime.now().strftime('%Y%m')
living_lab = "georgia"
zarr_out = f"s3://saferplaces.co/test/icisk/living_labs/test_ingestor/smhi/{issue_date}/{living_lab}_{data_dir}.zarr"

# initialize body data
data = {
    "inputs": {
        "data_dir": data_dir,
        "issue_date": issue_date,
        "living_lab": living_lab,
        "zarr_out": zarr_out
    }
}

# Print the details
print(f"Ingestor process: 'http://localhost/processes/{ingestor_process}/execution'")
print(f"Data directory: {data_dir}")
print(f"Current date: {issue_date}")
print(f"Living lab: {living_lab}")
print(f"Zarr output: {zarr_out}")
print(f"Data: {json.dumps(data, indent=2)}")

# curl command to invoke the ingestor using requests
response = requests.post(
    f"http://localhost/processes/{ingestor_process}/execution",
    headers={
        'accept': 'application/json',
        'Content-Type': 'application/json'
    },
    data=json.dumps(data)
)

print(f"Response status code: {response.status_code}")
print(f"Response body: {response.text}")
