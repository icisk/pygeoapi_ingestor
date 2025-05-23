import datetime
import fcntl
import io
import logging
import math
import os
import shutil
import time
import zipfile
from calendar import monthrange
from ftplib import FTP

import fsspec
import numpy as np
import requests
import xarray as xr
import yaml

logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)
# ch = logging.StreamHandler()
# ch.setLevel(logging.DEBUG)
# ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
# logger.addHandler(ch)


def floor_decimals(number, decimals=0):
    factor = 10**decimals
    return math.floor(number * factor) / factor


def ceil_decimals(number, decimals=0):
    factor = 10**decimals
    return math.ceil(number * factor) / factor


def days_in_month(date):
    return monthrange(date.year, date.month)[1]


def string_to_int_id(s):
    return int.from_bytes(s.encode(), "big")


def int_id_to_string(i):
    return i.to_bytes((i.bit_length() + 7) // 8, "big").decode()


def justfname(pathname):
    """
    justfname - returns the basename
    """
    return normpath(os.path.basename(normpath(pathname)))


def justpath(pathname, n=1):
    """
    justpath
    """
    for _ in range(n):
        pathname, _ = os.path.split(normpath(pathname))
    if pathname == "":
        return "."
    return normpath(pathname)


def normpath(pathname):
    """
    normpath
    """
    if not pathname:
        return ""
    pathname = os.path.normpath(pathname.replace("\\", "/")).replace("\\", "/")
    # patch for s3:// and http:// https://
    pathname = pathname.replace(":/", "://")
    return pathname


def juststem(pathname):
    """
    juststem
    """
    pathname = os.path.basename(pathname)
    root, _ = os.path.splitext(pathname)
    return root


# Create a representer for NumPy arrays
def numpy_array_representer(dumper, data):
    # Convert NumPy array to list and add a tag for NumPy array
    return dumper.represent_sequence("!numpy_array", data.tolist())


def read_config(config_path):
    with open(config_path, "r") as file:
        logger.info(f"reading config from '{config_path}")
        return yaml.safe_load(file)


def write_config(config_path, config_out):
    # Register the custom representer with PyYAML
    yaml.add_representer(np.ndarray, numpy_array_representer)
    with open(config_path, "w") as outfile:
        logger.info(f"locking file '{config_path}'")
        fcntl.flock(outfile, fcntl.LOCK_EX)
        try:
            logger.info(f"writing data to '{config_path}'")
            yaml.dump(config_out, outfile, default_flow_style=False, sort_keys=False)
        finally:
            logger.info(f"unlocking file '{config_path}'")
            fcntl.flock(outfile, fcntl.LOCK_UN)


def cleanup_data_temp():
    base_path = os.getenv("DATA_TEMP", "/tmp/data")
    shutil.rmtree(base_path)


def download_source(source):
    base_path = os.getenv("DATA_TEMP", "/tmp/data")

    if source.startswith("https"):
        res = requests.get(source)
        if source.endswith(".zip"):
            source_dir = os.path.basename(source).split(".")[0]
            out_dir = os.path.join(base_path, source_dir)
            os.makedirs(out_dir, exist_ok=True)
            with zipfile.ZipFile(io.BytesIO(res.content)) as zip:
                zip.extractall(out_dir)
        if source.endswith(".gpkg"):
            source_dir = os.path.basename(source)
            out_dir = os.path.join(base_path, source_dir)
            os.makedirs(base_path, exist_ok=True)
            with open(out_dir, "wb") as file:
                file.write(res.content)

    return out_dir


def check_running_jobs(retry=1, total_retries=1, time_out=10):
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
        job_list = res.json().get("jobs", [])
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch jobs: {e}")
        return False

    running_job_count = 0
    running_job = False
    for job in job_list:
        if job["status"] in running_job_status_list:
            running_job_count += 1
            if running_job_count > 1:
                running_job = True
                break

    if running_job and retry < total_retries:
        time.sleep(time_out)
        if not check_running_jobs(retry=retry + 1, total_retries=total_retries, time_out=time_out):
            return False

    return running_job


def list_ftp_files(server, user, passwd, source):
    try:
        ftp = FTP(server)
        ftp.login(user=user, passwd=passwd)
        items = ftp.nlst(source)
        ftp.quit()
        return items
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return None


def download_ftp_file(server, user, passwd, source, out_path):
    try:
        ftp = FTP(server)
        ftp.login(user=user, passwd=passwd)
        out_dir = justpath(out_path, 1)
        os.makedirs(out_dir, exist_ok=True)
        with open(out_path, "wb") as local_file:
            ftp.retrbinary(f"RETR {source}", local_file.write)
        ftp.quit()

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return None


def download_ftp_data(server, user, passwd, source, out_path):
    try:
        # Connect to the FTP server
        ftp = FTP(server)
        ftp.login(user=user, passwd=passwd)
        ftp.set_pasv(True)
        logger.info(f"Connected to '{server}' as user '{user}'")

        items = ftp.nlst(source)

        for item in items:
            logger.info(item)
            local_path = os.path.join(out_path, item)
            os.makedirs(os.path.join(out_path, source), exist_ok=True)

            with open(local_path, "wb") as local_file:
                ftp.retrbinary(f"RETR {item}", local_file.write)
            logger.info(f"Downloaded: '{item}' to '{local_path}'")

        # Close the connection
        ftp.quit()

    except Exception as e:
        logger.error(f"An error occurred: {e}")


def get_latest_forecast(server, user, passwd, fc_root):
    """
    returns start date of latest datetime (year, month) as datetime
    returns path to latest dataset
    """
    ftp = FTP(server)
    ftp.login(user=user, passwd=passwd)
    ftp.set_pasv(True)
    logger.info(f"Connected to '{server}' as user '{user}'")

    root_items = ftp.nlst(fc_root)
    latest_year = max([p for p in root_items])
    year_items = ftp.nlst(latest_year)
    latest_month = max([p for p in year_items])
    dt = datetime.datetime(year=int(os.path.basename(latest_year)), month=int(os.path.basename(latest_month)), day=1)
    return dt, latest_month


def get_data_from_cloud(zarr_path, alternate_root, otc_endpoint, otc_key, otc_secret):
    mapper = fsspec.get_mapper(
        zarr_path, alternate_root=alternate_root, endpoint_url=otc_endpoint, key=otc_key, secret=otc_secret
    )
    return xr.open_zarr(mapper)
