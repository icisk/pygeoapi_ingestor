import json
import os
import requests
import sys
import time

def invoke_ingestor_process(execute_url, data, logger):
    success = False
    n_tries = 0
    max_tries = int(os.getenv('INVOKE_MAX_TRIES', 5))
    sleep_seconds = int(os.getenv('INVOKE_SLEEP_SECONDS', 10))
    logger.info(f"Starting execution trigger with '{max_tries}' max tries and '{sleep_seconds}'s of intermediate sleep.")
    while not success and n_tries < max_tries:
        n_tries += 1
        logger.info(f"[{n_tries}/{max_tries}]: Send POST to '{execute_url}")
        try:
            response = requests.post(execute_url,
                headers={
                    'accept': 'application/json',
                    'Content-Type': 'application/json'
                },
                data=json.dumps(data)
            )
            logger.info(f"Response status code: {response.status_code}")
            logger.info("Response body       :")
            logger.info("---------------------")
            logger.info(f"{response.text}")
            logger.info("---------------------")

            if response.status_code >= 200 and response.status_code < 300:
                #TODO next iteration: for long running tasks implement async processes and use /jobs/<job-id>
                success = True

            elif n_tries < max_tries:
                #TODO add check for response code, hence we retry not for all errors, but only specific ones
                logger.info(f"Retrying in {sleep_seconds}s seconds...")
                time.sleep(sleep_seconds)

            else:
                logger.error(f"Failed to ingest data. Stopped after '{max_tries}' retries")
                sys.exit(52)

        except Exception as e:
            logger.error(f"Try #{n_tries}. Failed to invoke the ingestor: {e}")
            logger.error(f"Retrying in {sleep_seconds}s...")

            time.sleep(sleep_seconds)
