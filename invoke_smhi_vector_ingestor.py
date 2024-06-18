import requests
import json
from datetime import datetime
import logging
import sys

logging.basicConfig(
    format="[%(levelname)s] %(asctime)s %(message)s",
    level=logging.DEBUG,
    stream=sys.stdout)
logger = logging.getLogger(__name__)

# ingestor id
ingestor_process = 'ingestor-cds-vector-process'

# parameters for the ingestor
data_dir = "seasonal_forecast"
issue_date = datetime.now().strftime('%Y%m')
living_lab = "georgia"
# zarr_out = f"s3://saferplaces.co/test/icisk/living_labs/test_ingestor/smhi/{issue_date}/{living_lab}_{data_dir}.zarr"

# initialize body data
data = {
    "inputs": {
        "data_dir": data_dir,
        "issue_date": issue_date,
        "living_lab": living_lab
        # "zarr_out": zarr_out
    }
}

# Print the details
logger.debug(f"Ingestor process: 'http://localhost/processes/{ingestor_process}/execution'")
logger.debug(f"Data directory: {data_dir}")
logger.debug(f"Current date: {issue_date}")
logger.debug(f"Living lab: {living_lab}")
# logger.debug(f"Zarr output: {zarr_out}")
logger.debug(f"Data: {json.dumps(data, indent=2)}")

# curl command to invoke the ingestor using requests
response = requests.post(
    f"http://localhost/processes/{ingestor_process}/execution",
    headers={
        'accept': 'application/json',
        'Content-Type': 'application/json'
    },
    data=json.dumps(data)
)

logger.debug(f"Response status code: {response.status_code}")
logger.debug(f"Response body: {response.text}")
