import requests
import json
from datetime import datetime
import yaml
import time
import logging
import sys

logger = logging.getLogger(__name__)


with open('/scheduler.yaml', 'r') as file:
    config = yaml.safe_load(file)

creafforecast_config = config['living_lab']['scheduler']['creafforecast']

ingestor_process = 'ingestor-creafforecast-process'

data = {
  "inputs": {
    "data_path": creafforecast_config['data_path'],
    "zarr_out": creafforecast_config['zarr_out']
  }
}

execute_url = f"http://localhost/processes/{ingestor_process}/execution"

logger.debug(f"Ingestor process: '{execute_url}'")
logger.error(json.dumps(data))

success = False
n_tries = 0
max_tries = 5
while not success and n_tries < max_tries:
    n_tries += 1
    logger.debug(f"[{n_tries}/{max_tries}]: Send POST to '{execute_url}")
    try:
        response = requests.post(execute_url,
            headers={
                'accept': 'application/json',
                'Content-Type': 'application/json'
            },
            data=json.dumps(data)
        )
        success = True
    except Exception as e:
        logger.error(f"Try #{n_tries}. Failed to invoke the ingestor: {e}")
        logger.error("Retrying in 10 seconds...")

        time.sleep(10)

logger.debug(f"Response status code: {response.status_code}")
logger.debug(f"Response body: {response.text}")