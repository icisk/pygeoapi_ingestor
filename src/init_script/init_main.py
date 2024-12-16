import os
import s3fs
import yaml
import logging
import fcntl
from sqlalchemy import create_engine, inspect


logger = logging.getLogger('init config check')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(ch)


class InitialContainerCheck():
    def __init__(self,
                 config_path: str,
                 s3OTC: dict) -> None:
        self.config_path = config_path
        self.s3OTC = s3OTC

        self.config = self.read_config()
        self.resources = self.extract_data_source()
        self.s3 = s3fs.S3FileSystem(endpoint_url = self.s3OTC['FSSPEC_S3_ENDPOINT_URL'],
                                    key = self.s3OTC['FSSPEC_S3_KEY'],
                                    secret = self.s3OTC['FSSPEC_S3_SECRET'],
                                    anon=False,
                                  )

        self.config_out = self.create_new_config()

    def read_config(self):
        logger.debug(f"reading config from '{self.config_path}")
        with open(self.config_path, 'r') as file:
            data = yaml.safe_load(file)
        return data

    def write_config(self):
        logger.debug(f"writing config to '{self.config_path}'")
        with open(self.config_path, 'w') as outfile:
            logger.debug(f"locking file '{self.config_path}'")
            fcntl.flock(outfile, fcntl.LOCK_EX)
            try:
                logger.debug(f"writing data to '{self.config_path}'")
                yaml.dump(self.config_out, outfile, default_flow_style=False)
            finally:
                logger.debug(f"unlocking file '{self.config_path}'")
                fcntl.flock(outfile, fcntl.LOCK_UN)


    def extract_data_source(self):
        logger.debug("looking for data resources")
        res = {}
        for resource in self.config['resources']:
            if 'providers' in self.config['resources'][resource]:
                if 'data' in self.config['resources'][resource]['providers'][0]:
                    res[resource] = self.config['resources'][resource]['providers'][0]['data']
        logger.debug(f"found {len(res)} data resources")
        return(res)


    def create_new_config(self):
        # s3 = s3fs.S3FileSystem()
        logger.debug(f"checking dataset availability")
        poped_ds = []
        new_config = self.config.copy()
        for dataset, data in self.resources.items():
            #FIXME: nur s3 checken wenn s3 xarray-edr
            #TODO das ganze für andere quellen auch bitte danke
            if self.config['resources'][dataset]['providers'][0]['name'] in ['xarray-edr', 'S3GeoJSONProvider.S3GeoJSONProvider']:
                if not self.s3.exists(data):
                    logger.debug(f"dataset '{dataset}' NOT available at '{data}")
                    new_config['resources'].pop(dataset)
                    logger.debug(f"removed '{dataset}'")
                    poped_ds.append(dataset)
                else:
                    logger.debug(f"dataset '{dataset}' available at '{data}")
            elif self.config['resources'][dataset]['providers'][0]['name'] == 'PostgreSQL':
                db = data
                #db['host'] = 'localhost'
                engine = create_engine(f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['dbname']}")
                inspector = inspect(engine)
                if self.config['resources'][dataset]['providers'][0]['table'] not in inspector.get_table_names():
                    logger.debug(f"dataset '{dataset}' NOT available")
                    new_config['resources'].pop(dataset)
                    logger.debug(f"removed '{dataset}'")
                    poped_ds.append(dataset)
                else:
                    logger.debug(f"dataset '{dataset}' available")

        logger.debug(f"found {len(poped_ds)} dataset to be unavailable: {', '.join(poped_ds)}")
        return new_config



if __name__ == '__main__':
    config = os.environ.get('PYGEOAPI_CONFIG', default= '/pygeoapi/config/local.config.yml', )
    #config = '/work/PROJEKTE/ICISK/pygeoapi_ingestor/src/init_script/test.config.yaml'

    # s3 = {'FSSPEC_S3_ENDPOINT_URL': 'https://obs.eu-de.otc.t-systems.com',
    #       'FSSPEC_S3_KEY': '',
    #       'FSSPEC_S3_SECRET': ''}

    s3 = {'FSSPEC_S3_ENDPOINT_URL': os.environ.get(key='FSSPEC_S3_ENDPOINT_URL'),
          'FSSPEC_S3_KEY': os.environ.get(key='FSSPEC_S3_KEY'),
          'FSSPEC_S3_SECRET': os.environ.get(key='FSSPEC_S3_SECRET')}

    logger.debug(f"config: '{config}'")

    checker = InitialContainerCheck(config, s3)
    checker.write_config()
