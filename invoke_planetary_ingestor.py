import requests
import json
from datetime import datetime
import yaml
import logging
import sys

logging.basicConfig(
    format="[%(levelname)s] %(asctime)s %(message)s",
    level=logging.DEBUG,
    stream=sys.stdout)
logger = logging.getLogger(__name__)

# TODO: Implement the Planetary ingestor
logger.debug("Hello Planetary")

with open('/scheduler.yaml', 'r') as file:
    config = yaml.safe_load(file)

planetary_config = config['living_lab']['scheduler']['planetary']
logger.debug(planetary_config)

# ingestor id
ingestor_process = 'ingestor-planetary-process'

# # parameters for the ingestor
# data_dir = "seasonal_forecast"
# issue_date = datetime.now().strftime('%Y%m')
# living_lab = "georgia"
# zarr_out = f"s3://saferplaces.co/test/icisk/living_labs/test_ingestor/planetary/{issue_date}/{living_lab}_{data_dir}.zarr"



# initialize body data
data = {
    "inputs": {
        "dataset": planetary_config['dataset'],
        "collections": planetary_config['collections'],
        "varname": planetary_config['varname'],
        # "models": planetary_config['models'],
        "bbox": bbox,
        "date_start": str(planetary_config['date_start']),
        "date_end": str(planetary_config['date_end']),
        "query": planetary_config['query'],
        # "file_out": planetary_config['file_out'],
        "zarr_out": planetary_config['zarr_out'],
    }
}

logger.debug(data)
logger.debug("----")
# Print the details
logger.debug(f"Ingestor process: 'http://localhost/processes/{ingestor_process}/execution'")
# logger.debug(f"Data directory: {data_dir}")
# logger.debug(f"Current date: {issue_date}")
# logger.debug(f"Living lab: {living_lab}")
# logger.debug(f"Zarr output: {zarr_out}")
# logger.debug(f"Data: {json.dumps(data, indent=2)}")

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
