from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from filelock import Timeout, FileLock
import yaml

import os

import pandas as pd
import geopandas as gpd
import xarray as xr
import numpy as np
import json
import s3fs
import requests
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
    'id': 'danger levels creation',
    'title': {
        'en': 'dangerlvls',
    },
    'description': {
        'en': 'preocesses forecast data to identify drought threats'},
    'jobControlOptions': ['sync-execute', 'async-execute'],
    'keywords': ['process'],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'information',
        'href': 'https://example.org/process',
        'hreflang': 'en-US'
    }],
    'inputs': {
        'result_json': {
            'title': 'result',
            'description': 'The URL of the result',
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
            "result_json": "www.j.son"
        }
    }
}

class DangerLevelProcessProcessor(BaseProcessor):
    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

        self.zarr_file = 'https://52n-i-cisk.obs.eu-de.otc.t-systems.com/data-ingestor/netherlands/knmi_obs_forecast.zarr'
        self.mask_file = '/pygeoapi/secondary_process_data/rijn_mask.nc'
        self.out_file = None
        self.otc_key = os.environ.get(key='FSSPEC_S3_KEY')
        self.otc_secret = os.environ.get(key='FSSPEC_S3_SECRET')
        self.otc_endpoint = os.environ.get(key='FSSPEC_S3_ENDPOINT_URL')
        self.bucket_name = os.environ.get(key='DEFAULT_BUCKET')

    def rijnland_precip_def(self):
        rj_mask = xr.open_dataset(self.mask_file)
        ds = xr.open_zarr(self.zarr_file)
        weekly = ds['time'].resample(time='1W').max()
        time = weekly.time.values[:-1]
        weeks = np.array([t.astype('datetime64[W]').item().isocalendar()[1] for t in time])
        months = np.array([t.astype('datetime64[M]').item().month for t in time])
        mask_vals = rj_mask['mask'].where(rj_mask['mask'] == 666).values
        vals = [ds['p_def_q50'].sel(time=t).where(mask_vals).max().values for t in time]

        return weeks, months, vals

    def categorize_precip_def_rijnland(self, val):
        if val < 50:
            return "darkblue"
        if val >= 50 and val < 100:
            return "blue"
        if val >= 100 and val < 125:
            return "green"
        if val >= 125 and val < 150:
            return "yellow"
        if val >= 150 and val < 175:
            return 'orange'
        if val >= 175 and val < 200:
            return 'red'
        if val >= 200:
            return 'darkred'

    def execute(self, data):
        mimetype = 'application/json'
        self.out_file = data.get('result_json')
        self.token = data.get('token')

        LOGGER.debug(f"checking process inputs")
        if self.out_file is None:
            raise ProcessorExecuteError('Cannot process without a json path')

        LOGGER.debug(f"checking token")
        if self.token != os.getenv("INT_API_TOKEN", "token"):
            #FIXME passender error?
            LOGGER.error("WRONG INTERNAL API TOKEN")
            raise ProcessorExecuteError('ACCESS DENIED wrong token')
        LOGGER.debug(f'{requests.get("https://52n-i-cisk.obs.eu-de.otc.t-systems.com/llrijnland/rijnland_example_data_10_24.zip").status_code}')

        LOGGER.debug(f"start")
        rj_weeks, rj_month, rj_vals = self.rijnland_precip_def()
        rj_cat = [self.categorize_precip_def_rijnland(val) for val in rj_vals]
        rijn_dict = {int(item[0]): item[1] for item in [x for x in zip(rj_weeks, rj_cat)]}
        LOGGER.debug(f"have dict {rijn_dict}")

        json_data = json.dumps(rijn_dict)

        s3 = s3fs.S3FileSystem(
            key=os.environ.get('FSSPEC_S3_KEY'),
            secret=os.environ.get('FSSPEC_S3_SECRET'),
            client_kwargs={'endpoint_url': os.environ.get('FSSPEC_S3_ENDPOINT_URL')}
        )

        with s3.open(f'{self.bucket_name}/{self.out_file}', 'w') as f:
            LOGGER.debug(f"start writing")
            f.write(json_data)

        outputs = {
            'id': 'll_nl_danger_levels',
            'value': self.out_file
        }
        LOGGER.debug(f"return")
        return mimetype, outputs

    def __repr__(self):
        return f'<DangerLevelProcessProcessor> {self.name}'