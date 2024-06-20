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
# import cfgrib
# import ecmwflibs
import logging
import sys

logging.basicConfig(
    format="[%(levelname)s] %(asctime)s %(message)s",
    level=logging.DEBUG,
    stream=sys.stdout)
logger = logging.getLogger(__name__)


LOGGER = logging.getLogger(__name__)
load_dotenv(find_dotenv())
#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.2.0',
    'id': 'ingestor-process',
    'title': {
        'en': 'Ingestor Process',
    },
    'description': {
        'en': 'Ingestor Process is a process that fetches data from an FTP server and stores it in a Zarr file in an S3 bucket.'
          'The process is used to ingest data from the SMHI FTP server and store it in an S3 bucket.'
          'The process requires the following inputs: issue_date, data_dir, living_lab, zarr_out.'
          'The process fetches the data from the FTP server, reads the NetCDF files, and stores the data in a Zarr file in an S3 bucket.'
          'The process returns the URL of the Zarr file in the S3 bucket.'
          'The process also updates the pygeoapi configuration file to include the new dataset.'},
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
            "dataset": "cems-glofas-forecast",
            "query": {
                "system_version": "operational",
                "hydrological_model": "lisflood",
                "product_type": "ensemble_perturbed_forecasts",
                "variable": "river_discharge_in_the_last_24_hours",
                "year": "2024",
                "month": "04",
                "day": "28",
                "leadtime_hour": [
                    "24",
                    "48",
                    "72",
                    "96",
                    "120",
                    "144",
                    "168",
                    "192",
                    "216",
                    "240",
                    "264",
                    "288",
                    "312",
                    "336",
                    "360",
                    "384",
                    "408",
                    "432",
                    "456",
                    "480",
                    "504",
                    "528",
                    "552",
                    "576",
                    "600",
                    "624",
                    "648",
                    "672",
                    "696",
                    "720"
                ],
                "format": "netcdf4.zip",
                "area": [
                    44.8,
                    10,
                    43.8,
                    12
                ]
            },
            "file_out": "/tmp/cds-glofas-forecast.netcdf4.zip"
        }
    }
}


def generate_days_list(data_inizio, data_fine):
    data_corrente = data_inizio
    while data_corrente <= data_fine:
        yield data_corrente
        data_corrente += timedelta(days=1)


def fetch_dataset(dataset, query, file_out, date=None, engine='h5netcdf'):
    URL = 'https://cds.climate.copernicus.eu/api/v2'
    KEY = os.environ.get('CDSAPI_KEY')
    c = cdsapi.Client(url=URL, key=KEY)
    query_copy = query.copy()
    if date:
        query_copy['year'] = date.year
        query_copy['month'] = date.month
        query_copy['day'] = date.day
    # Hindcast data request
    c.retrieve(
        dataset,    # 'seasonal-monthly-single-levels',
        query_copy,
        file_out)

    if file_out.endswith('.netcdf4.zip'):
        # Unzip the file

        with zipfile.ZipFile(file_out, 'r') as zip_ref:
            local_file = f"{file_out.split('.netcdf4.zip')[0]}.nc"
            zip_ref.extractall(local_file)

        data = xr.open_dataset(f"{local_file}/data.nc", engine=engine)
    # elif file_out.endswith('.grib'):
    #     data = xr.open_dataset(file_out,engine="cfgrib")

    for var in data.data_vars:
        data[var] = data[var].expand_dims(dim='time')
    data.attrs['long_name'] = dataset

    return data


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

    def execute(self, data):

        mimetype = 'application/json'

        # get parameters from query params
        dataset = data.get('dataset')
        query = data.get('query')
        file_out = data.get('file_out', os.path.join(f"{tempfile.gettempdir()}",f"copernicus_data_{str(int(datetime.now().timestamp()))}.nc"))
        zarr_out = data.get('zarr_out')
        engine = data.get('engine', 'h5netcdf')
        s3_save = data.get('s3_save', False)
        start_date = data.get('date_start', None)
        end_date = data.get('date_end', None)

        if dataset is None:
            raise ProcessorExecuteError('Cannot process without a dataset')
        if query is None:
            raise ProcessorExecuteError('Cannot process without a query')

        
        if zarr_out and zarr_out.startswith('s3://'):
            s3_save = True
            s3 = s3fs.S3FileSystem(anon=True)
            # Check if the path already exists
            if s3.exists(zarr_out):
                raise ProcessorExecuteError(f'Path {zarr_out} already exists')
        else:
            if s3_save:
                bucket_name = os.environ.get("DEFAULT_BUCKET")
                remote_path = os.environ.get("DEFAULT_REMOTE_DIR")
                zarr_out = f's3://{bucket_name}/{remote_path}{dataset}_cds_{int(datetime.now().timestamp())}.zarr'
            else:
                zarr_out = f'/pygeoapi/{dataset}_cds_{int(datetime.now().timestamp())}.zarr'

        if start_date and end_date:

            datetime_start = datetime.strptime(start_date, '%Y-%m-%d')
            datetime_end = datetime.strptime(end_date, '%Y-%m-%d')
            # get list of days between start_date and end_date
            days = list(generate_days_list(datetime_start, datetime_end))

            datasets = []
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(fetch_dataset, dataset, query, f"{file_out.split('.netcdf4.zip')[0]}_{day.isoformat()}.netcdf4.zip", day): day for day in days}
                for future in as_completed(futures):
                    day = futures[future]
                    try:
                        out_data = future.result()
                        datasets.append(out_data)
                    except Exception as exc:
                        logger.error(f"{day} generated an exception: {exc}")
            data = xr.concat(datasets,dim='time')
        else:
            # check if year, month and day are in the query
            if 'year' in query and 'month' in query and 'day' in query:
                logger.debug('Fetching data for a specific date', query['year'], query['month'], query['day'])
            else:
                query['year'] = str(datetime.now().year)
                query['month'] = str(datetime.now().month)
                query['day'] = str(datetime.now().day)
            data = fetch_dataset(dataset, query, file_out, engine=engine)

        if s3_save:
            store = s3fs.S3Map(root=zarr_out, s3=s3, check=False)
        else:
            store = zarr_out

        data.to_zarr(store=store,
                    consolidated=True,
                    mode='w')
        # get min/max values for longitude, latitude and time
        min_x = float(data.coords['longitude'].min().values)
        max_x = float(data.coords['longitude'].max().values)
        min_y = float(data.coords['latitude'].min().values)
        max_y = float(data.coords['latitude'].max().values)

        min_time = data.coords['time'].min().values
        max_time = data.coords['time'].max().values

        # convert np.datetime64 to datetime object
        datetime_max = datetime.fromtimestamp(max_time.tolist()/1e9,tz=timezone.utc)
        datetime_min = datetime.fromtimestamp(min_time.tolist()/1e9,tz=timezone.utc)

        with open('/pygeoapi/local.config.yml', 'r') as file:
            config = yaml.safe_load(file)

        config['resources'][f"{dataset}_{datetime_min}_{datetime_max}"] = {
            'type': 'collection',
            'title': f"{dataset}_{datetime_min}_{datetime_max}",
            'description': f'CDS {dataset} data from {datetime_min} to {datetime_max}',
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
                    'time_field': 'time',
                    'format': {'name': 'zarr', 'mimetype': 'application/zip'},
                    # 'options': {
                    #     's3': {'anon': True, 'requester_pays': False}
                    # }
                }
            ]
        }

        if s3_save:
            config['resources'][dataset]['providers'][0]['options'] = {
                's3': {'anon': True, 'requester_pays': False}
            }

        with  open('/pygeoapi/local.config.yml', 'w') as outfile:
            yaml.dump(config, outfile, default_flow_style=False)

        outputs = {
            'id': 'ingestor-process',
            'value': zarr_out
        }
        return mimetype, outputs

    def __repr__(self):
        return f'<IngestorCDSProcessProcessor> {self.name}'
