import fcntl
import io
import logging
import os
import requests
import shutil
import yaml
import zipfile


logger = logging.getLogger('init config check')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(ch)


def read_config(config_path):
        with open(config_path, 'r') as file:
            logger.debug(f"reading config from '{config_path}")
            return(yaml.safe_load(file))


def write_config(config_path, config_out):
    with open(config_path, 'w') as outfile:
        logger.debug(f"locking file '{config_path}'")
        fcntl.flock(outfile, fcntl.LOCK_EX)
        try:
            logger.debug(f"writing data to '{config_path}'")
            yaml.dump(config_out, outfile, default_flow_style=False)
        finally:
            logger.debug(f"unlocking file '{config_path}'")
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

    return(out_dir)

