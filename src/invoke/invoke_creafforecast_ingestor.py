import requests
import json
from datetime import datetime
import yaml
import time
import logging
import os
import sys
from utils import invoke_ingestor_process

logging.basicConfig(
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    level=logging.DEBUG,
    stream=sys.stdout)
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

api_root = os.getenv("API_ROOT", "http://localhost/")

execute_url = f"{api_root}processes/{ingestor_process}/execution"

logger.debug(f"Ingestor process: '{execute_url}'")
logger.error(json.dumps(data))


invoke_ingestor_process(execute_url=execute_url, data=data, logger=logger)