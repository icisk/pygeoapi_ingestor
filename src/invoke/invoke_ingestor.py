import argparse
import json
import logging
import os
import sys

from utils import invoke_ingestor_process


# EXAMPLE ARG
# {"process": "ingestor-creafforecast-process",
#  "payload":{
#    "inputs":{
#      "data_source": "file:///data/creafforecast/",
#      "zarr_out": "s3://52n-i-cisk/data-ingestor/creaf_forecast.zarr"
#    }
#  } }

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

api_root = os.getenv("API_ROOT", "http://localhost/")
execute_url = f"{api_root}processes/{ingestor_process}/execution"

logger.info(f"Ingestor process: '{execute_url}' ")
logger.info(f"Payload: '{json.dumps(payload)}' ")

token = os.getenv("INT_API_TOKEN", "token")
payload['inputs']['token'] = token

logger.info(f"added secret token to payload!")


invoke_ingestor_process(execute_url=execute_url, data=payload, logger=logger)
