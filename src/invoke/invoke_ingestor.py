import argparse
import json
import logging
import os
import sys
import time
from utils import invoke_ingestor_process


# EXAMPLE input-file.json
# ---
# {
#   "process": "ingestor-creafforecast-process",
#   "payload": {
#     "inputs": {
#       "data_source": "file:///data/creafforecast/",
#       "zarr_out": "s3://52n-i-cisk/data-ingestor/creaf_forecast.zarr"
#     }
#   }
# }
# ---

parser = argparse.ArgumentParser(description="reads .json file that contains ingestor process name 'process' and process inputs 'payload' ")
parser.add_argument("json_path")
args = parser.parse_args()


logging.basicConfig(
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    level=logging.NOTSET,
    stream=sys.stdout,
    force=True)
logger = logging.getLogger(__name__)


with open(args.json_path, 'r') as f:
    data = json.load(f)

ingestor_process = data['process']
payload = data['payload']
if ingestor_process == "ingestor-cds-process":
    area = payload['inputs']['query']['area']
    variable = payload['inputs']['query']['variable']
    if isinstance(variable, list):
        # if variable is a list, take all the elements and join them with '-'
        variable = "-".join(variable)
    dataset = payload['inputs']['dataset']
    year = time.strftime("%Y")
    month = time.strftime("%m")
    day = time.strftime("%d")
    payload['inputs']['query']['year'] = [year]
    zarr_out = payload['inputs']['zarr_out']
    payload['inputs']['query']['month'] = [month]
    # if dataset == "seasonal-original-single-levels":
    #     payload['inputs']['query']['day'] = [day]
    payload['inputs']['zarr_out'] = f"{zarr_out.split('.zarr')[0]}-{dataset}_{variable}_{year}{month}{day}.zarr"   

api_root = os.getenv("API_ROOT", "http://localhost:5000/")
execute_url = f"{api_root}processes/{ingestor_process}/execution"

logger.info(f"Ingestor process: '{execute_url}' ")
logger.info(f"Payload: '{json.dumps(payload)}' ")

token = os.getenv("INT_API_TOKEN", "token")
payload['inputs']['token'] = token

logger.info("added secret token to payload!")


invoke_ingestor_process(execute_url=execute_url, data=payload, logger=logger)
