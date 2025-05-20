import json
import logging
import os
import sys

logging.basicConfig(
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    # log everything possible
    level=logging.NOTSET,
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(__name__)

#
#   Check environment variables
#
logger.debug("Checking environment variables")

PACKAGE_KEY = "PYGEOAPI_INGESTOR_PROCESSOR_PACKAGE"
package = os.getenv(key=PACKAGE_KEY, default="pygeoapi_ingestor_plugin")
logger.debug(f"    {PACKAGE_KEY}: {package}")

CLASS_KEY = "PYGEOAPI_INGESTOR_PROCESSOR_CLASS"
processor_class_name = os.getenv(key=CLASS_KEY)

if not processor_class_name or len(processor_class_name) == 0:
    logger.error(f"Mandatory environment variable '{CLASS_KEY}' not correctly configured: '{processor_class_name}'.")
    sys.exit(1)
logger.debug(f"    {CLASS_KEY}  : {processor_class_name}")

INPUTS_KEY = "PYGEOAPI_K8S_MANAGER_INPUTS"
raw_inputs = os.getenv(INPUTS_KEY)
if raw_inputs is None or len(raw_inputs) == 0:
    logger.error(f"Mandatory environment variable '{INPUTS_KEY}' not correctly configured: '{raw_inputs}'")
    sys.exit(2)


def hide_secrets_before_logging(inputs_json: dict) -> dict:
    def hide_value_if_required(key, value):
        return "*" if any(trigger in key.lower() for trigger in ["secret", "key", "password", "token"]) else value

    return {key: hide_value_if_required(key, value) for key, value in inputs_json.items()}


logger.debug(f"    {INPUTS_KEY}       : {hide_secrets_before_logging(json.loads(raw_inputs))}")

try:
    #
    #   Init processor
    #
    logger.debug(f"Try to load class '{package}.{processor_class_name}'")
    processor_class = getattr(__import__(package, fromlist=[processor_class_name]), processor_class_name)
    processor = processor_class(processor_def={"name": f"{package}.{processor_class_name}"})
    #
    #   Execute processor
    #
    inputs = json.loads(raw_inputs)
    logger.debug("Executing process with loaded inputs...")
    mimetype, result = processor.execute(data=inputs)
    #
    #   Process results
    #
    logger.debug("Processing results...")
    if "value" in result.keys() and "error" in str(result["value"]).lower():
        logger.error(f"Error during processing: mimetype: '{mimetype}'; Result: '{str(result)}'")
        sys.exit(3)
    logger.info(f"Processing finished. Mimetype: '{mimetype}'; Result: '{str(result)}'")
except Exception as e:
    logger.error(f"Error '{type(e)}' during processing: '{e}'")
    sys.exit(4)
