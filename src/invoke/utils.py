import json
import requests
import sys
import time

def invoke_ingestor_process(execute_url, data, logger):
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
            logger.debug(f"Response status code: {response.status_code}")
            logger.debug(f"Response body       : {response.text}")

            if response.status_code >= 200 and response.status_code < 300:
                #TODO next iteration: for long running tasks implement async processes and use /jobs/<job-id>
                success = True

            elif n_tries < max_tries:
                logger.info("Retrying in 10 seconds...")
                time.sleep(10)

            else:
                logger.error(f"Failed to ingest creaf data. Stopped after {max_tries} retries")
                sys.exit(52)

        except Exception as e:
            logger.error(f"Try #{n_tries}. Failed to invoke the ingestor: {e}")
            logger.error("Retrying in 10 seconds...")

            time.sleep(10)
