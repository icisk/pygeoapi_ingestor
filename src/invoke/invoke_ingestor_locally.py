import logging
import json
import os
import sys

logging.basicConfig(
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    # log everything possible
    level=logging.NOTSET,
    stream=sys.stdout,
    force=True)
logger = logging.getLogger(__name__)

#
#   Check environment variables
#
logger.debug(f"Checking environment variables")
CLASS_KEY = "PYGEOAPI_INGESTOR_PROCESSOR_CLASS"
processor_class_name = os.getenv(key=CLASS_KEY)

if not processor_class_name or len(processor_class_name) == 0:
    logger.error(f"Mandatory environment variable '{CLASS_KEY}' not correctly configured: '{processor_class_name}'.")
    sys.exit(1)
logger.debug(f"{CLASS_KEY}: {processor_class_name}")

INPUTS_KEY = "PYGEOAPI_K8S_MANAGER_INPUTS"
if raw_inputs := os.getenv(INPUTS_KEY) == None:
    logger.error(f"Mandatory environment variable '{INPUTS_KEY}' not configured: '{raw_inputs}'")
    sys.exit(2)
logger.debug(f"{INPUTS_KEY}: {raw_inputs}")

PACKAGE_KEY="PYGEOAPI_INGESTOR_PROCESSOR_PACKAGE"
package = os.getenv(
    key=PACKAGE_KEY,
    default="pygeoapi_ingestor_plugin"
)
logger.debug(f"{PACKAGE_KEY}: {package}")

try:
    #
    #   Init processor
    #
    logger.debug(f"Try to load class '{package}.{processor_class_name}'")
    processor_class = getattr(__import__(package, fromlist=[processor_class_name]), processor_class_name)
    processor = processor_class(processor_def={"name":f"{package}.{processor_class_name}"})
    #
    #   Execute processor
    #
    inputs = json.loads(raw_inputs)
    logger.debug(f"Inputs parsed: '{str(inputs)}'")
    logger.debug(f"Executing process...")
    mimetype, result = processor.execute(data=inputs)
    #
    #   Process results
    #
    logger.debug(f"Processing results...")
    if "value" in result.keys() and "error" in str(result["value"]).lower():
        logger.error(f"Error during processing: mimetype: '{mimetype}'; Result: '{str(result)}'")
        sys.exit(3)
    logger.info(f"Processing finished. Mimetype: '{mimetype}'; Result: '{str(result)}'")
except Exception as e:
    logger.error(f"Error during processing: '{e}'")
    sys.exit(4)
