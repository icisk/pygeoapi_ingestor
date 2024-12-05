import fcntl
import io
import logging
import os
import requests
import shutil
import yaml
import zipfile
import time
import numpy as np

logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)
# ch = logging.StreamHandler()
# ch.setLevel(logging.DEBUG)
# ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
# logger.addHandler(ch)


# Create a representer for NumPy arrays
def numpy_array_representer(dumper, data):
    # Convert NumPy array to list and add a tag for NumPy array
    return dumper.represent_sequence('!numpy_array', data.tolist())

def read_config(config_path):
        with open(config_path, 'r') as file:
            logger.info(f"reading config from '{config_path}")
            return(yaml.safe_load(file))


def write_config(config_path, config_out):

    # Register the custom representer with PyYAML
    yaml.add_representer(np.ndarray, numpy_array_representer)
    with open(config_path, 'w') as outfile:
        logger.info(f"locking file '{config_path}'")
        fcntl.flock(outfile, fcntl.LOCK_EX)
        try:
            logger.info(f"writing data to '{config_path}'")
            yaml.dump(config_out, outfile, default_flow_style=False, sort_keys=False)
        finally:
            logger.info(f"unlocking file '{config_path}'")
            fcntl.flock(outfile, fcntl.LOCK_UN)

def cleanup_data_temp():
    base_path = os.getenv("DATA_TEMP", '/tmp/data')
    shutil.rmtree(base_path)


def download_source(source):
    base_path = os.getenv("DATA_TEMP", '/tmp/data')

    if source.startswith('https'):
        res = requests.get(source)
        if source.endswith('.zip'):
            source_dir = os.path.basename(source).split(".")[0]
            out_dir = os.path.join(base_path, source_dir)
            os.makedirs(out_dir, exist_ok=True)
            with zipfile.ZipFile(io.BytesIO(res.content)) as zip:
                zip.extractall(out_dir)
        if source.endswith('.gpkg'):
            source_dir = os.path.basename(source)
            out_dir = os.path.join(base_path, source_dir)
            os.makedirs(base_path, exist_ok=True)
            with open(out_dir, 'wb') as file:
                file.write(res.content)

    return(out_dir)


def check_running_jobs(retry=1,total_retries=1, time_out=10):
    """
    Check if there are running jobs outside the one that have called the function
    Parameters
    ----------
    retry : int
        Number of retries
    total_retries : int
        Total number of retries
    time_out : int
        Time out in seconds
    Returns
    -------
    bool
        True if there are running jobs outside the one that have called the function, False otherwise
    """
    running_job_status_list = ["accepted", "running"]
    api_root = "http://localhost/"
    request_url = f"{api_root}jobs"
    try:
        res = requests.get(request_url)
        res.raise_for_status()
        job_list = res.json().get('jobs', [])
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch jobs: {e}")
        return False

    running_job_count = 0
    running_job = False
    for job in job_list:
        if job['status'] in running_job_status_list:
            running_job_count += 1
            if running_job_count > 1:
                running_job = True
                break

    if running_job and retry < total_retries:
        time.sleep(time_out)
        if not check_running_jobs(retry=retry+1, total_retries=total_retries, time_out=time_out):
            return False

    return running_job
