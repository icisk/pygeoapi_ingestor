import json
import os
import sys
import time

import requests


def log_response(logger, response):
    logger.info(f"Response status code: {response.status_code}")
    logger.info("Response header     :")
    logger.info("---------------------")
    max_header_length = max(len(header) for header in response.headers.keys())
    for header, value in response.headers.items():
        logger.info(f"{header:<{max_header_length}}: '{value}'")
    logger.info("---------------------")
    logger.info("Response body       :")
    logger.info("---------------------")
    logger.info(f"{response.text}")
    logger.info("---------------------")


def invoke_ingestor_process(execute_url, data, logger):
    success = False
    n_tries = 0
    max_tries = int(os.getenv("INVOKE_MAX_TRIES", 5))
    sleep_seconds = int(os.getenv("INVOKE_SLEEP_SECONDS", 10))
    logger.info(
        f"Starting execution trigger with '{max_tries}' max tries and '{sleep_seconds}'s of intermediate sleep."
    )
    headers = {"accept": "application/json", "Content-Type": "application/json"}
    if "async" in data["inputs"] and data["inputs"]["async"] is True:
        headers["Prefer"] = "respond-async"
        logger.info("Setting Prefer header to 'respond-async' for async processing")
    logger.info(f"Headers: {headers}")
    while not success and n_tries < max_tries:
        n_tries += 1
        logger.info(f"[{n_tries}/{max_tries}]: Send POST to '{execute_url}")
        try:
            response = requests.post(execute_url, headers=headers, data=json.dumps(data))
            log_response(logger, response)

            if response.status_code >= 200 and response.status_code < 300:
                if "async" in data["inputs"] and data["inputs"]["async"] is True:
                    # response header Location should contain link to job, if body is None
                    job_url = response.headers["Location"]
                    job_url = f"http://localhost:80/{job_url[job_url.index('jobs/') : ]}"    # ???: This shouldn't be necessary... check both docker env-vars and pygeoapi-config
                    
                    job_status = None
                    logger.info(f"Starting job state checker with job URL '{job_url}'")

                    # see https://docs.ogc.org/is/18-062r2/18-062r2.html#_11de0cd6-7c05-4951-b0f0-2a801c554ac2
                    while job_status not in ["failed", "successful", "dismissed"]:
                        job_status_response = requests.get(f"{job_url}?f=json")
                        if job_status_response.status_code >= 200 and job_status_response.status_code < 300:
                            job_status = job_status_response.json().get("status")
                            if job_status in ["failed", "successful", "dismissed"]:
                                log_response(logger, job_status_response)  # TODO remove later
                                break
                            logger.info(f"Status '{job_status}'. Sleeping {sleep_seconds}s...")
                            time.sleep(sleep_seconds)
                        else:
                            log_response(logger, job_status_response)
                            logger.error("Could not get job status info")
                            sys.exit(52)

                    if job_status in ["failed", "dismissed"]:
                        logger.error(
                            f"Failed to ingest data with job_status '{job_status}'. Stopped after '{n_tries}/{max_tries}' retries."
                        )
                        logger.error(f"Error Message: '{job_status_response.json().get('message')}'")
                        sys.exit(52)

                    # get result
                    job_result_response = requests.get(f"{job_url}/results?f=json")
                    log_response(logger, job_result_response)

                success = True

            elif n_tries < max_tries:
                # TODO add check for response code, hence we retry not for all errors, but only specific ones
                logger.info(f"Retrying in {sleep_seconds}s seconds...")
                time.sleep(sleep_seconds)

            else:
                logger.error(f"Failed to ingest data. Stopped after '{max_tries}' retries")
                sys.exit(52)

        except Exception as e:
            logger.error(f"Invoke #{n_tries} failed because of: '{e}'.")
            logger.error(f"Retrying in {sleep_seconds}s...")

            time.sleep(sleep_seconds)
