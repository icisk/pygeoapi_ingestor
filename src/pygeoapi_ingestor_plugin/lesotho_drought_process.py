import fcntl
import logging
import os
import tempfile
import numpy as np
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
from filelock import FileLock
import s3fs
import yaml
from droughtpipeline.pipeline import Pipeline
from droughtpipeline.secrets import Secrets
from droughtpipeline.settings import Settings
from datetime import date, datetime, timedelta, timezone
import xarray as xr
from droughtpipeline.save_zarr import save_zarr

from pygeoapi_ingestor_plugin.utils import read_config
# =================================================================
#
# Authors: Valerio Luzzi <valluzzi@gmail.com>
#
# Copyright (c) 2023 Valerio Luzzi
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

logger = logging.getLogger(__name__)

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.2.0',
    'id': 'lesotho-drought-process',
    'title': {
        'en': 'Lesotho Drought Ingestor Process',
    },
    'description': {
        'en': 'Lesotho Drought Ingestor Process'},
    'jobControlOptions': ['sync-execute', 'async-execute'],
    'keywords': ['ingestor process', 'smhi', 'ftp', 'geojson'],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'information',
        'href': 'https://example.org/process',
        'hreflang': 'en-US'
    }],
    'inputs': {
        'datetime_start': {
            'title': 'Issue Date',
            'description': 'The issue date of the forecast',
            'schema': {
                'type': 'string'
            }
        },
        'zarr_out': {
            'title': 'Zarr output',
            'description': 'The output Zarr file',
            'schema': {
                'type': 'string'
            }
        },
        'cron_invocation': {
            'title': 'Cron Invocation',
            'description': 'Invoke the process as a cron job',
            'schema': {
                'type': 'boolean'
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
            'description': 'The URL of the geojson file in the S3 bucket or local path',
            'schema': {
                'type': 'string'
            }
        }
    },
    'example': {
        "inputs": {
            "datetime_start": "2025-02-01T00:00:00",
            "token": "ABC123XYZ666",
        }
    }
}


class LesothoDroughtProcessor(BaseProcessor):
    """
    Ingestor Processor example
    """

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition

        :returns: pygeoapi.process.ingestor_process.IngestorSMHIVectorProcessProcessor
        """

        super().__init__(processor_def, PROCESS_METADATA)
        self.config_file = os.environ.get(default='/pygeoapi/serv-config/local.config.yml', key='PYGEOAPI_SERV_CONFIG')
        self.id = 'lesotho-drought-process'

    def run_drought_pipeline(
        self, country, prepare, extract, forecast, send, save, datetimestart, datetimeend, debug
    ):
        # datetimestart = datetime.strptime(datetimestart, "%Y-%m-%dT%H:%M:%S")
        # datetimeend = datetime.strptime(datetimeend, "%Y-%m-%dT%H:%M:%S")
        logging.info(f"Running drought pipeline for {country}")
        logging.info(f"Start date: {datetimestart}")
        logging.info(f"End date: {datetimeend}")
        os.makedirs("/tmp/data/input", exist_ok=True)
        os.makedirs("/tmp/data/output", exist_ok=True)
        pipe = Pipeline(
            country=country,
            settings=Settings("data/lesotho/config.yaml"),
            secrets=Secrets(".env"),
            climate_region_code_path="./data/lesotho/LSO_climate_region.json"
        )
        pipe.run_pipeline(
            prepare=prepare,
            extract=extract,
            forecast=forecast,
            send=send,
            save=save,
            debug=debug,
            datetimestart=datetimestart,
            datetimeend=datetimeend,
        )

    def execute(self, data):

        mimetype = 'application/json'
        datetime_start, zarr_out, token = self._extract_parameters(data)

        self._validate_inputs(datetime_start, token)
        
        # Check if zarr_out already exists
        msg = self._check_s3_path_exists(zarr_out, s3_is_anon_access=False)
        if msg:
            return mimetype, {'id': self.id, 'value': msg}
        
        try:
            self.run_drought_pipeline(country="LSO", prepare=True, extract=True, forecast=True, send=False,save=False, debug=True,datetimestart=datetime_start, datetimeend=datetime_start.month+6)
        except Exception as e:
            logger.error(f"Error running pipeline: {e}")
            return mimetype, {'id': self.id, 'value': f"Error running pipeline: {e}"}
        dataset = save_zarr("/tmp/data/output/", zarr_out, datetime_start)
        
        self.update_config(dataset, zarr_out, self.config_file)
        
        outputs = {
            'id': self.id,
            'out': zarr_out
        }
        return mimetype, outputs

    def _extract_parameters(self, data):
        """Extract parameters from the input data."""
        DEFAULT_BUCKET = os.getenv("DEFAULT_BUCKET")
        DEFAULT_REMOTE_DIR = os.getenv("DEFAULT_REMOTE_DIR")
        token = data.get('token')
        cron_invocation = data.get('cron_invocation')
        if cron_invocation:
            datetime_start = datetime.today()
            datetime_start = datetime_start.replace(day=1,hour=0,minute=0,second=0,microsecond=0)
        else:
            datetime_start = data.get('datetime_start')
            datetime_start = datetime.strptime(datetime_start, "%Y-%m-%dT%H:%M:%S")
        zarr_out = data.get('zarr_out', f"s3://{DEFAULT_BUCKET}/{DEFAULT_REMOTE_DIR}lesotho_drought_forecast_{datetime_start.isoformat()}.zarr")

        return datetime_start, zarr_out, token

    def _validate_inputs(self, datetime_start, token):
        """Validate input parameters."""
        # if service is None:
        #     raise ProcessorExecuteError('Cannot process without a service')
        if token is None:
            raise ProcessorExecuteError('Identify yourself with valid token!')
        if datetime_start > datetime.now():
            raise ProcessorExecuteError('Cannot process future data')
        if token != os.getenv("INT_API_TOKEN", "token"):
            logger.error(f"WRONG INTERNAL API TOKEN {token} ({type(token)}) != {os.getenv('INT_API_TOKEN', 'token')} ({type(os.getenv('INT_API_TOKEN', 'token'))})")
            raise ProcessorExecuteError('ACCESS DENIED: wrong token')

    def get_coordinates_name(self, data):
        lat_names = {"latitude", "lat", "y"}
        lon_names = {"longitude", "lon", "x"}
        lat_name = next((name for name in data.coords if name.lower() in lat_names), None)
        lon_name = next((name for name in data.coords if name.lower() in lon_names), None)
        return lat_name, lon_name
    
    def get_timevar_name(self, data):
        time_names = {'time', 'valid_time', 'forecast_period'}
        time_name = next((name for name in data.coords if name.lower() in time_names), None)
        return time_name

    def read_config(self, config_path):
        with open(config_path, 'r') as file:
            logger.info(f"reading config from '{config_path}")
            return(yaml.safe_load(file))
        
    # Create a representer for NumPy arrays
    def numpy_array_representer(self, dumper, data):
        # Convert NumPy array to list and add a tag for NumPy array
        return dumper.represent_sequence('!numpy_array', data.tolist())
    
    def write_config(self, config_path, config_out):

        # Register the custom representer with PyYAML
        yaml.add_representer(np.ndarray, self.numpy_array_representer)
        with open(config_path, 'w') as outfile:
            logger.info(f"locking file '{config_path}'")
            fcntl.flock(outfile, fcntl.LOCK_EX)
            try:
                logger.info(f"writing data to '{config_path}'")
                yaml.dump(config_out, outfile, default_flow_style=False, sort_keys=False)
            finally:
                logger.info(f"unlocking file '{config_path}'")
                fcntl.flock(outfile, fcntl.LOCK_UN)

    def update_config(self, data, file_out, config_file, s3_is_anon_access=False):

        LAT, LON = self.get_coordinates_name(data=data)
        TIME = self.get_timevar_name(data=data)

        # get min/max values for longitude, latitude and time
        min_x = float(data.coords[LON].min().values)
        max_x = float(data.coords[LON].max().values)
        min_y = float(data.coords[LAT].min().values)
        max_y = float(data.coords[LAT].max().values)

        logger.debug(f"min_x: {min_x}, max_x: {max_x}, min_y: {min_y}, max_y: {max_y}")

        min_time = data.coords[TIME].values.min()
        max_time = data.coords[TIME].values.max()


        # convert np.datetime64 to datetime object .strftime("%Y%m")
        datetime_max = datetime.fromtimestamp(max_time.tolist()/1e9,tz=timezone.utc)
        datetime_min = datetime.fromtimestamp(min_time.tolist()/1e9,tz=timezone.utc)
        datetime_min_ym = datetime_min.strftime("%Y%m")
        datetime_max_ym = datetime_max.strftime("%Y%m")


        logger.info(f"datetime_min: {datetime_min}, datetime_max: {datetime_max}")

        # THIS MUST BE THE SAME IN ALL PROCESSES UPDATING THE SERV CONFIG
        lock = FileLock(f"{self.config_file}.lock", thread_local=False)

        with lock:
            config = self.read_config(config_file)

            dataset_pygeoapi_identifier = f"lesotho_drought_forecast_{datetime_min_ym}"
            zarr_out = file_out

            logger.info(f"resource identifier and title: '{dataset_pygeoapi_identifier}'")
            
            time_field = "time"
            dataset_definition = {
                'type': 'collection',
                'title': dataset_pygeoapi_identifier,
                'description': f"Lesotho drought forecast from {datetime_min} to {datetime_max}",
                'keywords': ['country'],
                'extents': {
                    'spatial': {
                        'bbox': [min_x, min_y, max_x, max_y],
                        'crs': 'http://www.opengis.net/def/crs/OGC/1.3/CRS84'
                    },
                    'temporal': {
                        'begin': datetime_min,
                        'end': datetime_max
                    }
                },
                'providers': [
                    {
                        'type': 'edr',
                        'name': 'xarray-edr',
                        'data': zarr_out,
                        'x_field': LON,
                        'y_field': LAT,
                        'time_field': time_field,
                        'format': {'name': 'zarr', 'mimetype': 'application/zip'}
                    }
                ]
            }

            logger.info(f"dataset definition to add: '{dataset_definition}'")

            config['resources'][dataset_pygeoapi_identifier] = dataset_definition

            endpoint_url = os.environ.get(default="https://obs.eu-de.otc.t-systems.com", key='FSSPEC_S3_ENDPOINT_URL')
            alternate_root = zarr_out.split("s3://")[1]

            if s3_is_anon_access:
                config['resources'][dataset_pygeoapi_identifier]['providers'][0]['options'] = {
                    's3': {
                        'anon': True,
                        'requester_pays': False
                    }
                }
            else:
                config['resources'][dataset_pygeoapi_identifier]['providers'][0]['options'] = {
                    's3': {
                        'anon': False,
                        'alternate_root': alternate_root,
                        'endpoint_url': endpoint_url,
                        'requester_pays': False
                    }
                }
            self.write_config(config_path=config_file, config_out=config)
        
    def _check_s3_path_exists(self, zarr_out, s3_is_anon_access):
        """Check if the S3 path already exists."""
        msg = ""
        if zarr_out.startswith('s3://'):
            s3 = s3fs.S3FileSystem()
            if s3.exists(zarr_out):
                if zarr_out in str(read_config(self.config_file)['resources']):
                    msg = f"Path {zarr_out} already exists in bucket and config"
                else:
                    data = xr.open_zarr(zarr_out)
                    self.update_config(data, zarr_out, self.config_file, s3_is_anon_access)
                    msg = f"Path {zarr_out} already exists updates config at '{self.config_file}'."
        return msg
    
    def __repr__(self):
        return f'<LesothoDroughtProcessor> {self.name}'
