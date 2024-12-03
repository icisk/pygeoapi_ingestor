from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from filelock import Timeout, FileLock
import yaml

import os

import pandas as pd
import geopandas as gpd

from sqlalchemy import create_engine
import geoalchemy2
import psycopg2


from .utils import download_source

from osgeo import gdal

import logging
from dotenv import load_dotenv, find_dotenv





LOGGER = logging.getLogger(__name__)

load_dotenv(find_dotenv())

PROCESS_METADATA = {
    'version': '0.2.0',
    'id': 'aemet_stations_ingestor',
    'title': {
        'en': 'aemet_stations',
    },
    'description': {
        'en': 'downloads temp and precip data ingests ts in postgis-db'},
    'jobControlOptions': ['sync-execute', 'async-execute'],
    'keywords': ['ingestor process'],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'information',
        'href': 'https://example.org/process',
        'hreflang': 'en-US'
    }],
    'inputs': {
        'zarr_out': {
            'title': 'Zarr Output',
            'description': 'The URL of the Zarr file in the S3 bucket',
            'schema': {
                'type': 'string'
            }
        },
        'token': {
            'title': 'secret token',
            'description': 'identify yourself',
            'schema': {
                'type': 'string'
            }
        }
    },
    'outputs': {
        'id': {
            'title': 'ID',
            'description': 'The ID of the process execution',
            'schema': {
                'type': 'string'
            }
        },
        'value': {
            'title': 'Value',
            'description': 'The URL of the Zarr file in the S3 bucket',
            'schema': {
                'type': 'string'
            }
        }
    },
    'example': {
        "inputs": {
            "zarr_out": "s3://example/target/bucket.zarr",
            "token": "ABC123XYZ666"
        }
    }
}




class IngestorAEMETSTATIONSProcessProcessor(BaseProcessor):
    """
    Ingestor Processor

    joins tiff-data and saves it to zarr format and uploads it to s3 bucket

    Resource Requirements:
    - CPU: 3
    - RAM: 12G
    """

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition

        :returns: pygeoapi.process.ingestor_process.IngestorCREAFHISTORICProcessProcessor
        """

        super().__init__(processor_def, PROCESS_METADATA)
        self.config_file = os.environ.get(default='/pygeoapi/serv-config/local.config.yml', key='PYGEOAPI_SERV_CONFIG')
        self.title = 'AEMET_stations'
        self.db_user = os.environ.get(key='DB_USER')
        self.db_password = os.environ.get(key='DB_PASSWORD')
        self.db_host = os.environ.get(key='DB_HOST')
        self.db_port = os.environ.get(key='DB_PORT')
        self.db_database = None ## os.environ.get(key='DB_DATABASE')
        self.data_url = None
        self.variable = None
        self.variables = ['precip', 't_min', 't_max', 't_mean']
        self.data_path = None
        self.csv_file_name = None

    def read_config(self):
        with open(self.config_file, 'r') as file:
            LOGGER.debug("read config")
            return(yaml.safe_load(file))

    def write_config(self, new_config):
        with  open(self.config_file, 'w') as outfile:
            yaml.dump(new_config, outfile, default_flow_style=False)
        LOGGER.debug("updated config")


    def update_config(self):
        min_x, min_y, max_x, max_y = [float(val) for val in self.data.total_bounds]

        # THIS MUST BE THE SAME IN ALL PROCESSES UPDATING THE SERV CONFIG
        lock = FileLock(f"{self.config_file}.lock")

        with lock:

            config= self.read_config()
            config['resources'][f'{self.title}_{self.variable}'] = {
                'type': 'collection',
                'title': f'{self.title}_{self.variable}',
                'description': f'historic station data of {self.variable}',
                'keywords': ['country'],
                'extents': {
                    'spatial': {
                        'bbox': [min_x, min_y, max_x, max_y],
                        'crs': 'http://www.opengis.net/def/crs/EPSG/0/4326'
                    },
                },
                'providers':
                    {
                        'type': 'feature',
                        'name': 'PostgreSQL',
                        'data': {
                            'host': self.db_host,
                            'port': self.db_port,
                            'dbname': self.db_database,
                            'user': self.db_user,
                            'password': self.db_password,
                            'search_path': ['osm', 'public']
                        },
                        'id_field': 'CODI_INM',
                        'table': f'AEMET_{self.variable}',
                        'geom_field': 'geometry'
                    }
            }

            self.write_config(config)


    def check_config_if_ds_is_collection(self):
        config = self.read_config()
        return self.title in config['resources']

    def transform(self, csv):
        data = pd.read_csv(csv, sep=";", encoding="iso-8859-1")
        data['DATE'] = pd.to_datetime(data[['YEAR', 'MONTH']].assign(DAY=1))
        gdf = gpd.GeoDataFrame(data, geometry=gpd.GeoSeries.from_xy(data['MAP_X'], data['MAP_Y'], crs="EPSG:25830"))
        gdf = gdf.to_crs(epsg=4326)
        gdf['LAT'] = gdf.geometry.y
        gdf['LON'] = gdf.geometry.x


    def execute(self, data):
        mimetype = 'application/json'

        self.data_url = data.get('data_url')
        self.variable = data.get('variable')
        self.db_database = data.get('db_database')
        self.token = data.get('token')

        LOGGER.debug(f"checking process inputs")
        if self.data_url is None:
            raise ProcessorExecuteError('Cannot process without a zarr path')
        if self.variable not in self.variables:
            raise ProcessorExecuteError(f"'{self.variable}' not in '{self.variables}' check spelling ")
        if self.db_database is None:
            raise ProcessorExecuteError(f'give database name!')
        if self.token is None:
            raise ProcessorExecuteError('Identify yourself with valid token!')

        LOGGER.debug(f"checking token")
        if self.token != os.getenv("INT_API_TOKEN", "token"):
            #FIXME passender error?
            LOGGER.error("WRONG INTERNAL API TOKEN")
            raise ProcessorExecuteError('ACCESS DENIED wrong token')

        LOGGER.debug(f"selecting variable")
        if self.variable == 'precip':
            self.csv_file_name = "PRECIPITATION_monthlydata_GuadalquivirLL_1950_2019_v2.csv"
        if self.variable == 't_min':
            self.csv_file_name = 'MINTEMPERATURE_monthlydata_GuadalquivirLL_1990_2019_v2.csv'
        if self.variable == 't_mean':
            self.csv_file_name = 'MEANTEMPERATURE_monthlydata_GuadalquivirLL_1990_2019_v2.csv'
        if self.variable == 't_max':
            self.csv_file_name = 'MAXTEMPERATURE_monthlydata_GuadalquivirLL_1990_2019_v2.csv'

        self.data_path = download_source(self.data_url)

        self.data = self.transform(os.path.join(self.data_path, self.csv_file_name))

        engine = create_engine(f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_database}")
        self.data.to_postgis(f"AEMET_{self.variable}", engine, if_exists='replace', index=False)

        self.update_config()


        outputs = {
            'id': 'AEMET_stations_ingestor',
            'value': self.variable
        }

        return mimetype, outputs

    def __repr__(self):
        return f'<IngestorAEMETSTATIONSProcessProcessor> {self.name}'