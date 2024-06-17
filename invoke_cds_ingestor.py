import requests
import json
from datetime import datetime
import yaml
import time

with open('./scheduler.yaml', 'r') as file:
    config = yaml.safe_load(file)

cds_config = config['living_lab']['scheduler']['cds']

# ingestor id
ingestor_process = 'ingestor-cds-process'

# # parameters for the ingestor
# data_dir = "seasonal_forecast"
# issue_date = datetime.now().strftime('%Y%m')
# living_lab = "georgia"
# zarr_out = f"s3://saferplaces.co/test/icisk/living_labs/test_ingestor/cds/{issue_date}/{living_lab}_{data_dir}.zarr"


if 'date_start' in cds_config.keys() and 'date_end' in cds_config.keys():
    date_start = cds_config['date_start']
    date_end = cds_config['date_end']
    cds_config['query']['year'] = date_start.year
    cds_config['query']['month'] = date_start.month
    cds_config['query']['day'] = date_start.day


# initialize body data
data = {
    "inputs": {
        "dataset": cds_config['dataset'],
        "query": cds_config['query'],
        "file_out": cds_config['file_out'],
        # "zarr_out": zarr_out
    }
}

if 'date_start' in cds_config.keys() and 'date_end' in cds_config.keys():
    data['inputs']['date_start'] = date_start.strftime('%Y-%m-%d')
    data['inputs']['date_end'] = date_end.strftime('%Y-%m-%d')

print(f"Ingestor process: 'http://localhost/processes/{ingestor_process}/execution'")
# curl command to invoke the ingestor using requests
# try 5 times to invoke the ingestor if it fails wait 10 seconds before trying again

success = False
n_tries = 0
while not success and n_tries < 5:
    n_tries += 1
    try:
        response = requests.post(
            f"http://localhost/processes/{ingestor_process}/execution",
            headers={
                'accept': 'application/json',
                'Content-Type': 'application/json'
            },
            data=json.dumps(data)
        )
        success = True
    except Exception as e:
        print(f"Try #{n_tries}. Failed to invoke the ingestor: {e}")
        print("Retrying in 10 seconds...")

        time.sleep(10)


print(f"Response status code: {response.status_code}")
print(f"Response body: {response.text}")
