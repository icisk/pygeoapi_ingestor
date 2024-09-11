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
#      "data_path": "/data/creafforecast/",
#      "zarr_out": "s3://52n-i-cisk/data-ingestor/creaf_forecast.zarr"
#    }
#  } }

parser = argparse.ArgumentParser(description="accepts dict of ingestor process name 'process' and process inputs 'payload' ")
parser.add_argument("data")
args = parser.parse_args()


logging.basicConfig(
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    level=logging.DEBUG,
    stream=sys.stdout)
logger = logging.getLogger(__name__)


ingestor_process = args.data['process']
payload = args.data['payload']

api_root = os.getenv("API_ROOT", "http://localhost/")
execute_url = f"{api_root}processes/{ingestor_process}/execution"

logger.info(f"Ingestor process: '{execute_url}' ")
logger.info(f"Payload: '{json.dumps(payload)}' ")


invoke_ingestor_process(execute_url=execute_url, data=payload, logger=logger)
