import fcntl
import yaml
import logging


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