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

# curl -X POST -H "Content-Type: application/json" -d "{\"inputs\":{\"name\":\"valerio\"}}" http://localhost:5000/processes/ingestor-process/execution
# curl -X POST -H "Content-Type: application/json" -d "{\"inputs\":{\"name\":\"gdalinfo\"}}" http://localhost:5000/processes/k8s-process/execution

from ftplib import FTP
import logging, time
import yaml
import fsspec
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
import os
import s3fs
from datetime import datetime, timedelta, timezone
import xarray as xr
import json
import tempfile
from dotenv import load_dotenv, find_dotenv
import cdsapi
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import sys
from pygeoapi_ingestor_plugin.utils import check_running_jobs, write_config, read_config

LOGGER = logging.getLogger(__name__)

# Define constants for all days and all months
ALL_DAYS = [f"{day:02}" for day in range(1, 32)]
ALL_MONTHS = [f"{month:02}" for month in range(1, 13)]

load_dotenv(find_dotenv())
#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.2.0',
    'id': 'cds-ingestor-process',
    'title': {
        'en': 'CDS Ingestor Process',
    },
    'description': {
        'en': 'Ingestor process for Copernicus Climate Data Store (CDS) data',},
    'jobControlOptions': ['sync-execute', 'async-execute'],
    'keywords': ['ingestor process','cds'],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'information',
        'href': 'https://example.org/process',
        'hreflang': 'en-US'
    }],
    'inputs': {
        'dataset': {
            'title': 'Dataset',
            'description': 'CDS Dataset to fetch data from',
            'schema': {
                'type': 'string'
            }
        },
        'query': {
            'title': 'Query',
            'description': 'The query to fetch the data from the CDS API',
            'schema': {
                'type': 'string'
            }
        },
        'file_out': {
            'title': 'File out',
            'description': 'The path to the file where the data will be stored locally',
            'schema': {
                'type': 'string'
            }
        },
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
            "date_start": "2023-01-01",
            "date_end": "2024-01-01",
            "dataset": "cems-glofas-historical",
            "file_out": "/tmp/cds-glofas-historical.netcdf4.zip",
            "query": {
                "system_version": ["version_4_0"],
                "hydrological_model": ["lisflood"],
                "product_type": ["consolidated"],
                "variable": ["river_discharge_in_the_last_24_hours"],
                "hyear": ["2024"],
                "hmonth": ["01"],
                "hday": ["01"],
                "data_format": "netcdf",
                "download_format": "zip",
                "area": [45, 10, 44, 11]
            },
            "interval": "month",
            "zarr_out": "s3://mybucket/test/icisk/cds_rdis_test.zarr",
            "token": "ABC123XYZ666" 
        }
    }
}


def generate_days_list(data_inizio, data_fine):
    data_corrente = data_inizio
    while data_corrente <= data_fine:
        yield data_corrente
        data_corrente += timedelta(days=1)


def fetch_dataset(dataset, query, file_out, date=None, interval=None,engine='h5netcdf'):
    URL_CDS = 'https://cds.climate.copernicus.eu/api'
    URL_EWDS = 'https://ewds.climate.copernicus.eu/api'
    KEY = 'd08c3547-5017-4630-820f-57bb784f4973'
    client = cdsapi.Client(url=URL_CDS, key=KEY)

    query_copy = query.copy()
    
    # Adjust query based on the specified interval
    if interval == 'day':
        query_copy['hyear'] = [date.strftime('%Y')]
        query_copy['hmonth'] = [date.strftime('%m')]
        query_copy['hday'] = [date.strftime('%d')]
    elif interval == 'month':
        query_copy['hyear'] = [date.strftime('%Y')]
        query_copy['hmonth'] = [date.strftime('%m')]
        query_copy['hday'] = ALL_DAYS  # Include all days for the month
    elif interval == 'year':
        query_copy['hyear'] = [date.strftime('%Y')]
        query_copy['hmonth'] = ALL_MONTHS  # Include all months
        query_copy['hday'] = ALL_DAYS     # Include all days for each month
        
    try:
        data = client.retrieve(dataset, query_copy, file_out)
    except Exception as e:
        if "404 Client Error" in str(e):
            LOGGER.debug(f"Dataset {dataset} not found in {URL_CDS}")
            client = cdsapi.Client(url=URL_EWDS, key=KEY)
            data = client.retrieve(dataset, query_copy, file_out)
        else:
            LOGGER.error(f"Error fetching dataset {dataset}: {e}")
            return None

    if file_out.endswith('.zip'):
        extract_dir = file_out.split('.zip')[0]
        with zipfile.ZipFile(file_out, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        for file in os.listdir(extract_dir):
            if file.endswith('.nc'):
                file_nc = os.path.join(extract_dir, file)
        data = xr.open_dataset(file_nc, engine=engine)
    elif file_out.endswith('.nc'):
        data = xr.open_dataset(file_out)
    # elif file_out.endswith('.grib'):
    #     data = xr.open_dataset(file_out,engine="cfgrib")

    if dataset == 'cems-glofas-forecast':
        for var in data.data_vars:
            data[var] = data[var].expand_dims(dim='time')

    data.attrs['long_name'] = dataset

    return data


def update_config(data, dataset, zarr_out, config_file, s3_is_anon_access):
    # get min/max values for longitude, latitude and time
        min_x = float(data.coords['longitude'].min().values)
        max_x = float(data.coords['longitude'].max().values)
        min_y = float(data.coords['latitude'].min().values)
        max_y = float(data.coords['latitude'].max().values)

        min_time = data.coords['valid_time'].values.min()
        max_time = data.coords['valid_time'].values.max()

        # convert np.datetime64 to datetime object
        datetime_max = datetime.fromtimestamp(max_time.tolist()/1e9,tz=timezone.utc)
        datetime_min = datetime.fromtimestamp(min_time.tolist()/1e9,tz=timezone.utc)

        config = read_config(config_file)

        dataset_pygeoapi_identifier = f"{dataset}_{datetime_min.date()}_{datetime_max.date()}"

        config['resources'][dataset_pygeoapi_identifier] = {
            'type': 'collection',
            'title': dataset_pygeoapi_identifier,
            'description': f'CDS {dataset} data from {datetime_min.date()} to {datetime_max.date()}',
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
                    'x_field': 'longitude',
                    'y_field': 'latitude',
                    # 'time_field': 'time',
                    'format': {'name': 'zarr', 'mimetype': 'application/zip'}
                }
            ]
        }

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

        write_config(config_path=config_file, config_out=config)

class IngestorCDSProcessProcessor(BaseProcessor):
    """
    Ingestor Processor example
    """

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition

        :returns: pygeoapi.process.ingestor_process.IngestorCDSProcessProcessor
        """
        super().__init__(processor_def, PROCESS_METADATA)
        self.config_file = os.environ.get('PYGEOAPI_CONFIG_FILE', '/pygeoapi/local.config.yml')
        self.id = 'cds-ingestor-process'

    def execute(self, data):
        mimetype = 'application/json'

        # Extract parameters
        dataset, query, file_out, zarr_out, engine, s3_save, start_date, end_date, interval = self._extract_parameters(data)

        # Validate input
        self._validate_inputs(dataset, query, self.token)

        # Check for running jobs
        if check_running_jobs(total_retries=10, time_out=30):
            return mimetype, {'message': 'There are running jobs, please try again later'}

        # Determine S3 access type
        s3_is_anon_access = self._is_s3_anon_access()
        LOGGER.debug(f"Using anon S3 access? '{s3_is_anon_access}'")

        # Set up output paths
        zarr_out, s3_save = self._setup_output_paths(zarr_out, s3_save, dataset)

        # Check if S3 path already exists
        self._check_s3_path_exists(zarr_out)

        # Fetch data (either by date range or single query)
        data = self._fetch_data(dataset, query, file_out, engine, start_date, end_date, interval)

        # Save the data
        self._store_data(data, zarr_out, s3_save)

        # Update config and handle errors
        self._update_config_with_error_handling(data, dataset, zarr_out, s3_is_anon_access)

        # Return outputs
        outputs = {
            'id': self.id,
            'value': zarr_out
        }
        return mimetype, outputs

    def _extract_parameters(self, data):
        """Extract parameters from the input data."""
        dataset = data.get('dataset')
        query = data.get('query')
        file_out = data.get('file_out', os.path.join(f"{tempfile.gettempdir()}", f"copernicus_data_{int(datetime.now().timestamp())}.nc"))
        zarr_out = data.get('zarr_out')
        engine = data.get('engine', 'h5netcdf')
        s3_save = data.get('s3_save', False)
        start_date = data.get('date_start', None)
        end_date = data.get('date_end', None)
        self.token = data.get('token')
        interval = data.get('interval', None)
        return dataset, query, file_out, zarr_out, engine, s3_save, start_date, end_date, interval

    def _validate_inputs(self, dataset, query, token):
        """Validate input parameters."""
        if dataset is None:
            raise ProcessorExecuteError('Cannot process without a dataset')
        if query is None:
            raise ProcessorExecuteError('Cannot process without a query')
        if token is None:
            raise ProcessorExecuteError('Identify yourself with valid token!')

        if token != os.getenv("INT_API_TOKEN", "token"):
            LOGGER.error(f"WRONG INTERNAL API TOKEN {token} ({type(token)}) != {os.getenv('INT_API_TOKEN', 'token')} ({type(os.getenv('INT_API_TOKEN', 'token'))})")
            raise ProcessorExecuteError('ACCESS DENIED: wrong token')

    def _is_s3_anon_access(self):
        """Determine if S3 access should be anonymous."""
        s3_is_anon_access = os.environ.get('S3_ANON_ACCESS', 'True')
        return s3_is_anon_access == 'True'

    def _setup_output_paths(self, zarr_out, s3_save, dataset):
        """Set up file paths based on S3 configuration and dataset information."""
        if zarr_out and zarr_out.startswith('s3://'):
            s3_save = True
        else:
            if s3_save:
                bucket_name = os.environ.get("DEFAULT_BUCKET")
                remote_path = os.environ.get("DEFAULT_REMOTE_DIR")
                zarr_out = f's3://{bucket_name}/{remote_path}{dataset}_cds_{int(datetime.now().timestamp())}.zarr'
            else:
                if not zarr_out:
                    zarr_out = f'/pygeoapi/cds_data/{dataset}_cds_{int(datetime.now().timestamp())}.zarr'
        return zarr_out, s3_save

    def _check_s3_path_exists(self, zarr_out):
        """Check if the S3 path already exists."""
        if zarr_out.startswith('s3://'):
            s3 = s3fs.S3FileSystem()
            if s3.exists(zarr_out):
                raise ProcessorExecuteError(f'Path {zarr_out} already exists')

    def _fetch_data(self, dataset, query, file_out, engine, start_date, end_date,interval):
        """Fetch data from the dataset based on the date range or single query."""
        if start_date and end_date:
            datetime_start = datetime.strptime(start_date, '%Y-%m-%d')
            datetime_end = datetime.strptime(end_date, '%Y-%m-%d')
            dates = list(self.generate_dates_list(datetime_start, datetime_end, interval=interval))
            return self._fetch_data_by_range(dataset, query, file_out, dates, interval)
        else:
            LOGGER.debug(f"Fetching data for a specific date {query}")
            return fetch_dataset(dataset, query, file_out, engine=engine)

    def _fetch_data_by_range(self, dataset, query, file_out, dates, interval):
        datasets = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(fetch_dataset, dataset, query, f"{file_out.split('.netcdf4.zip')[0]}_{date.isoformat()}.netcdf4.zip", date, interval): date
                for date in dates
            }
            for future in as_completed(futures):
                date = futures[future]
                try:
                    out_data = future.result()
                    datasets.append(out_data)
                except Exception as exc:
                    LOGGER.error(f"{date} generated an exception: {exc}")
        return xr.concat(datasets, dim='time')

    def generate_dates_list(self, start_date, end_date, interval='day'):
        current_date = start_date
        while current_date <= end_date:
            yield current_date
            if interval == 'day':
                current_date += timedelta(days=1)
            elif interval == 'month':
                current_date = (current_date.replace(day=1) + timedelta(days=32)).replace(day=1)
            elif interval == 'year':
                current_date = current_date.replace(year=current_date.year + 1, month=1, day=1)

    def _store_data(self, data, zarr_out, s3_save):
        """Store the fetched data either in S3 or locally."""
        if s3_save:
            s3 = s3fs.S3FileSystem()
            store = s3fs.S3Map(root=zarr_out, s3=s3, check=False)
        else:
            store = zarr_out
        LOGGER.debug(f"Storing data to {store}, data type: {type(data)}")
        data.to_zarr(store=store, consolidated=True, mode='w')

    def _update_config_with_error_handling(self, data, dataset, zarr_out, s3_is_anon_access):
        """Update the config file and handle errors."""
        try:
            update_config(data, dataset, zarr_out, self.config_file, s3_is_anon_access)
        except Exception as e:
            LOGGER.error(f"Error updating config: {e}")

    def __repr__(self):
        return f'<IngestorCDSProcessProcessor> {self.name}'
