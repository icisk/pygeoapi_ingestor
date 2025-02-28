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

import itertools
import json
import logging
import os
import tempfile
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import cdsapi
import pandas as pd
import s3fs
import xarray as xr
from dotenv import load_dotenv, find_dotenv
# curl -X POST -H "Content-Type: application/json" -d "{\"inputs\":{\"name\":\"valerio\"}}" http://localhost:5000/processes/ingestor-process/execution
# curl -X POST -H "Content-Type: application/json" -d "{\"inputs\":{\"name\":\"gdalinfo\"}}" http://localhost:5000/processes/k8s-process/execution
from filelock import FileLock
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
from pygeoapi_ingestor_plugin.utils import write_config, read_config
from pygeoapi_ingestor_plugin.utils_azure import upload_file_to_azure

logger = logging.getLogger(__name__)

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
        self.config_file = os.environ.get(default='/pygeoapi/serv-config/local.config.yml', key='PYGEOAPI_SERV_CONFIG')
        self.id = 'cds-ingestor-process'

    def execute(self, data):
        mimetype = 'application/json'
        logger.info(f"|||  INFO LOG ||| \n {data} \n ||||||||||")

        # Extract parameters
        service, dataset, query, file_out, zarr_out, engine, s3_save, start_date, end_date, interval, living_lab = self._extract_parameters(data)

        # Validate input
        self._validate_inputs(service, dataset, query, self.token)

        # Determine S3 access type
        s3_is_anon_access = self._is_s3_anon_access()
        logger.info(f"Using anon S3 access? '{s3_is_anon_access}'")

        # Set up output paths
        zarr_out, s3_save = self._setup_output_paths(zarr_out, s3_save, dataset, living_lab)

        # Check if S3 path already exists
        msg = self._check_s3_path_exists(zarr_out, dataset, s3_is_anon_access, living_lab)
        if msg:
            return mimetype, {'id': self.id, 'value': msg}

        # Fetch data (either by date range or single query)
        data = self._fetch_data(service, dataset, query, file_out, engine, start_date, end_date, interval)

        if data is None:
            return mimetype, {'id': self.id, 'value': f'Error data for {dataset} not found'} 
        # Save the data
        self._store_data(data, zarr_out, s3_save, living_lab, dataset)

        # Update config and handle errors
        self._update_config_with_error_handling(data, dataset, zarr_out, s3_is_anon_access, living_lab)

        # Return outputs
        outputs = {
            'id': self.id,
            'value': zarr_out
        }
        return mimetype, outputs

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


    def update_config(self, data, dataset, file_out, config_file, s3_is_anon_access, living_lab):
        # Get variable name
        variable_name = list(data.data_vars)[0]

        LAT, LON = self.get_coordinates_name(data)
        TIME = self.get_timevar_name(data)

        # get min/max values for longitude, latitude and time
        min_x = float(data.coords[LON].min().values)
        max_x = float(data.coords[LON].max().values)
        min_y = float(data.coords[LAT].min().values)
        max_y = float(data.coords[LAT].max().values)

        logger.debug(f"min_x: {min_x}, max_x: {max_x}, min_y: {min_y}, max_y: {max_y}")

        # try:
        #     min_time = data.coords['valid_time'].values.min()
        #     max_time = data.coords['valid_time'].values.max()
        # except Exception as e:
        #     logger.error(f"Error getting min/max time: {e}")
        #     min_time = data.coords['forecast_period'].values.min()
        #     max_time = data.coords['forecast_period'].values.max()

        min_time = data.coords[TIME].values.min()
        max_time = data.coords[TIME].values.max()


        # convert np.datetime64 to datetime object .strftime("%Y%m")
        datetime_max = datetime.fromtimestamp(max_time.tolist()/1e9,tz=timezone.utc)
        datetime_min = datetime.fromtimestamp(min_time.tolist()/1e9,tz=timezone.utc)


        logger.info(f"datetime_min: {datetime_min}, datetime_max: {datetime_max}")

        # THIS MUST BE THE SAME IN ALL PROCESSES UPDATING THE SERV CONFIG
        lock = FileLock(f"{self.config_file}.lock", thread_local=False)

        with lock:
            config = read_config(config_file)

            dataset_pygeoapi_identifier = f"{dataset}_{datetime_min}_{living_lab}_{variable_name}"
            if file_out.endswith('.zarr'):
                zarr_out = file_out

                logger.info(f"resource identifier and title: '{dataset_pygeoapi_identifier}'")
                if dataset == "cems-glofas-seasonal":
                    time_field = "forecast_period"
                else:
                    time_field = "time"
                dataset_definition = {
                    'type': 'collection',
                    'title': dataset_pygeoapi_identifier,
                    'description': f'CDS {dataset} variable {variable_name} data from {datetime_min} to {datetime_max} for area [{min_x},{min_y},{max_x},{max_y}]',
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
            # if dataset == "cems-glofas-seasonal":
            elif file_out.endswith('.geojson'):
                # file_out = zarr_out.split('.zarr')[0]+'.geojson'
                resource_key = f"{dataset_pygeoapi_identifier}_stations"
                config['resources'][resource_key] = {
                    'type': 'collection',
                    'title': resource_key,
                    'description': f'CDS {dataset} variable {variable_name} data from {datetime_min} to {datetime_max} for area [{min_x},{min_y},{max_x},{max_y}]',
                    'keywords': [
                        living_lab,
                        'country',
                        'discharge',
                        'forecast',
                        'cds',
                        'seasonal'
                    ],
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
                    'providers': [{
                        'type': 'feature',
                        'name': 'S3GeoJSONProvider.S3GeoJSONProvider',
                        'data': file_out,
                        'id_field': 'id'
                    }]
                }
                
                config['resources'][resource_key]['providers'][0]['options'] = {
                    's3': {
                        'anon': True,
                        'requester_pays': False
                    }
                }
            write_config(config_path=config_file, config_out=config)


    def _extract_parameters(self, data):
        """Extract parameters from the input data."""
        service = data.get('service')
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
        cron_invocation = data.get('cron_invocation')
        living_lab = data.get('living_lab', None)
        variable = query['variable']
        if isinstance(variable, list):
            # if variable is a list, take all the elements and join them with '-'
            variable = "-".join(variable)
        if cron_invocation:
            year = time.strftime("%Y")
            month = time.strftime("%m")
            # day = time.strftime("%d")
            query['year'] = [year]
            query['month'] = [month]

        zarr_out = f"{zarr_out.split('.zarr')[0]}-{dataset}_{variable}_{query['year'][0]}{query['month'][0]}.zarr"   

        return service, dataset, query, file_out, zarr_out, engine, s3_save, start_date, end_date, interval, living_lab

    def _validate_inputs(self, service, dataset, query, token):
        """Validate input parameters."""
        # if service is None:
        #     raise ProcessorExecuteError('Cannot process without a service')
        if dataset is None:
            raise ProcessorExecuteError('Cannot process without a dataset')
        if query is None:
            raise ProcessorExecuteError('Cannot process without a query')
        if token is None:
            raise ProcessorExecuteError('Identify yourself with valid token!')

        if token != os.getenv("INT_API_TOKEN", "token"):
            logger.error(f"WRONG INTERNAL API TOKEN {token} ({type(token)}) != {os.getenv('INT_API_TOKEN', 'token')} ({type(os.getenv('INT_API_TOKEN', 'token'))})")
            raise ProcessorExecuteError('ACCESS DENIED: wrong token')

    def _is_s3_anon_access(self):
        """Determine if S3 access should be anonymous."""
        s3_is_anon_access = os.environ.get('S3_ANON_ACCESS', 'True')
        return s3_is_anon_access == 'True'

    def _setup_output_paths(self, zarr_out, s3_save, dataset, living_lab):
        """Set up file paths based on S3 configuration and dataset information."""
        if zarr_out and zarr_out.startswith('s3://'):
            s3_save = True
        else:
            if s3_save:
                bucket_name = os.environ.get("DEFAULT_BUCKET")
                remote_path = os.environ.get("DEFAULT_REMOTE_DIR")
                zarr_out = f's3://{bucket_name}/{remote_path}{dataset}_{living_lab}_cds_{int(datetime.now().timestamp())}.zarr'
            else:
                if not zarr_out:
                    zarr_out = f'/pygeoapi/cds_data/{dataset}_{living_lab}_cds_{int(datetime.now().timestamp())}.zarr'
        return zarr_out, s3_save

    def _check_s3_path_exists(self, zarr_out, dataset, s3_is_anon_access, living_lab):
        """Check if the S3 path already exists."""
        msg = None
        if zarr_out.startswith('s3://'):
            s3 = s3fs.S3FileSystem()
            if s3.exists(zarr_out):
                if zarr_out in str(read_config(self.config_file)['resources']):
                    msg = f"Path {zarr_out} already exists in bucket and config"
                else:
                    data = xr.open_zarr(zarr_out)
                    self.update_config(data, dataset, zarr_out, self.config_file, s3_is_anon_access, living_lab)
                    msg = f"Path {zarr_out} already exists updates config at '{self.config_file}'."
            if dataset == "cems-glofas-seasonal":
                geojson_out = zarr_out.split('.zarr')[0]+'.geojson'
                if s3.exists(geojson_out):
                    if geojson_out in str(read_config(self.config_file)['resources']):
                        msg = msg+f" Path {geojson_out} already exists in bucket and config"
                    else:
                        # self.upload_geojson(geojson_out, data)
                        data = xr.open_zarr(zarr_out)
                        self.update_config(data, dataset, geojson_out, self.config_file, s3_is_anon_access, living_lab)
        return msg

    def _fetch_data(self, service, dataset, query, file_out, engine, start_date, end_date,interval):
        """Fetch data from the dataset based on the date range or single query."""
        if start_date and end_date:
            datetime_start = datetime.strptime(start_date, '%Y-%m-%d')
            datetime_end = datetime.strptime(end_date, '%Y-%m-%d')
            dates = list(self.generate_dates_list(datetime_start, datetime_end, interval=interval))
            return self._fetch_data_by_range(service, dataset, query, file_out, dates, interval)
        elif dataset == "cems-glofas-seasonal":
            logger.info("Fetching data for cems-glofas-seasonal")
            return self.fetch_dataset_by_chunk(service, dataset, query, file_out, engine=engine)
        else:
            logger.info(f"Fetching data for a specific date {query}")
            return self.fetch_dataset(service, dataset, query, file_out, engine=engine)

    # Function to create chunks of leadtime_hour values
    def chunk_list(self, data, chunk_size):
        """Split a list into smaller chunks."""
        for i in range(0, len(data), chunk_size):
            yield data[i:i + chunk_size]
            
    # Function to handle individual chunk requests
    def fetch_chunk(self, chunk,base_request,service,dataset, file_out, engine):
        try:
            if service and service == 'EWDS':
                URL = 'https://ewds.climate.copernicus.eu/api'
            else:
                URL = 'https://cds.climate.copernicus.eu/api'

            KEY = os.getenv('CDSAPI_KEY')

            client = cdsapi.Client(url=URL, key=KEY)
            request = base_request.copy()
            request["leadtime_hour"] = chunk
            output_filename = f"{file_out}_chunk_{chunk[0]}-{chunk[-1]}.zip"

            logger.debug(f"Fetching data for leadtime_hours={chunk[0]} to {chunk[-1]}...")
            client.retrieve(dataset, request).download(output_filename)
            logger.debug(f"Downloaded: {output_filename}")

            if output_filename.endswith('.zip'):
                extract_dir = output_filename[:-4] 
                with zipfile.ZipFile(output_filename, 'r') as zip_ref:
                    zip_ref.extractall(extract_dir)
                for file in os.listdir(extract_dir):
                    if file.endswith('.nc'):
                        logger.debug(f"Extracted file: {extract_dir}\{file}")
                        file_nc = os.path.join(extract_dir, file)
                data = xr.open_dataset(file_nc, engine=engine)
            elif output_filename.endswith('.nc'):
                data = xr.open_dataset(output_filename, engine=engine)
            # elif file_out.endswith('.grib'):
            #     data = xr.open_dataset(file_out,engine="cfgrib")
            
            data.attrs['long_name'] = dataset
            data['forecast_period'] = data['valid_time']
            data.drop_vars('valid_time')
            return data
        except Exception as e:
            logger.error(f"Error fetching data for leadtime_hours={chunk[0]} to {chunk[-1]}: {e}")
            raise
        
    def fetch_dataset_by_chunk(self, service, dataset, query, file_out, engine):
        
        if len(query['leadtime_hour']) <= 30:
            logger.info(f"Fetching data for <=30 leadtime_hour {query}")
            xr_data = self.fetch_chunk(query['leadtime_hour'], query, service, dataset, file_out, engine)
        else:
            # List of leadtime_hour values
            leadtime_hours = [str(i) for i in range(24, 5161, 24)]  # From 24 to 5160 in steps of 24
            chunk_size = 54  # Number of leadtime_hours per request
            leadtime_chunks = list(self.chunk_list(leadtime_hours, chunk_size))

            logger.info(f"Fetching data for {len(leadtime_hours)} leadtime_hours in {len(leadtime_chunks)} chunks...")
            # Main execution block
            datasets = []
            with ThreadPoolExecutor(max_workers=5) as executor:  # Adjust max_workers as needed
                # Submit tasks to executor
                futures = {
                    executor.submit(self.fetch_chunk, chunk, query, service, dataset, file_out, engine): chunk       #fetch_chunk(self, chunk,base_request,service,dataset):
                    for chunk in leadtime_chunks
                }

                # Process completed futures
                for future in as_completed(futures):
                    chunk = futures[future]
                    try:
                        out_data = future.result()  # Get the result of the future
                        datasets.append(out_data)  # Collect successful outputs
                    except Exception as exc:
                        logger.error(f"Chunk {chunk[0]}-{chunk[-1]} generated an exception: {exc}")

                xr_data = xr.merge(datasets)
        xr_split_model_data = self.split_var_to_variables(xr_data, "dis24")
        logger.debug(xr_split_model_data)
        return xr_split_model_data

    def _fetch_data_by_range(self, service, dataset, query, file_out, dates, interval):
        datasets = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(self.fetch_dataset, service, dataset, query, f"{file_out.split('.netcdf4.zip')[0]}_{date.isoformat()}.netcdf4.zip", date, interval): date
                for date in dates
            }
            for future in as_completed(futures):
                date = futures[future]
                try:
                    out_data = future.result()
                    datasets.append(out_data)
                except Exception as exc:
                    logger.error(f"{date} generated an exception: {exc}")
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

    def _store_data(self, data, zarr_out, s3_save, living_lab, dataset):
        """Store the fetched data either in S3 or locally."""
        if living_lab == "lesotho":
            # create file nc from data
            nc_out_local = os.path.basename(zarr_out.split('.zarr')[0]+'.nc')
            logger.debug(f"Saving data to Lesotho Azure Blob Storage: {nc_out_local}")
            data.to_netcdf(nc_out_local)
            upload_file_to_azure(nc_out_local, sub_folder="cds")
        if s3_save:
            s3 = s3fs.S3FileSystem()
            store = s3fs.S3Map(root=zarr_out, s3=s3, check=False)
        else:
            store = zarr_out
        logger.info(f"Storing data to {store}, data type: {type(data)}")
        data.to_zarr(store=store, consolidated=True, mode='w')
        if dataset == "cems-glofas-seasonal":
            geojson_out = zarr_out.split('.zarr')[0]+'.geojson'
            # upload data as geojson
            self.upload_geojson(geojson_out, data)
            
    def upload_geojson(self, geojson_out, data):
        geojson_data = self.create_geojson(data)
        if geojson_out.startswith('s3://'):
            s3 = s3fs.S3FileSystem()
            with s3.open(geojson_out, 'w') as f:
                f.write(str(geojson_data))
                logger.info(f"Geojson data saved to {geojson_out}")

    def adjust_query(self, query, date, interval):
        if interval == 'day':
            query['hyear'] = [date.strftime('%Y')]
            query['hmonth'] = [date.strftime('%m')]
            query['hday'] = [date.strftime('%d')]
        elif interval == 'month':
            query['hyear'] = [date.strftime('%Y')]
            query['hmonth'] = [date.strftime('%m')]
            query['hday'] = ALL_DAYS  # Include all days for the month
        elif interval == 'year':
            query['hyear'] = [date.strftime('%Y')]
            query['hmonth'] = ALL_MONTHS  # Include all months
            query['hday'] = ALL_DAYS     # Include all days for each month
        return query

    def fetch_dataset(self, service, dataset, query, file_out, date=None, interval=None, engine='h5netcdf'):

        if service and service == 'EWDS':
            URL = 'https://ewds.climate.copernicus.eu/api'
        else:
            URL = 'https://cds.climate.copernicus.eu/api'

        KEY = os.getenv('CDSAPI_KEY')

        client = cdsapi.Client(url=URL, key=KEY)

        query_copy = query.copy()

        # Adjust query based on the specified interval
        query_copy = self.adjust_query(query_copy, date, interval)

        logger.info(f"service     : '{service}'")
        logger.info(f"URL         : '{URL}'")
        logger.info(f"dataset     : '{dataset}'")
        logger.info(f"CDSAPI query: '{query_copy}'")

        try:
            data = client.retrieve(dataset, query_copy, file_out)
        except Exception as e:

            logger.error(f"Error fetching dataset {dataset} from service '{service}' ('{URL}'): '{e}'")
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
        elif dataset == "seasonal-original-single-levels":
            if 'total_precipitation' in query['variable']:
                varname = 'tp'
            elif '2m_temperature' in query['variable']:
                varname = 't2m'
                
            data = self.split_var_to_variables(data, varname)
            if 'time' in data.variables:
                data = data.drop_vars("time")  # Drop existing 'time' if necessary
            if 'forecast_period' in data.variables:
                data["forecast_period"] = data["valid_time"]
                data = data.rename({'forecast_period': 'time'})
            else:
                data['step'] = data['valid_time']
                data = data.rename({'step': 'time'})

        data.attrs['long_name'] = dataset

        return data

    def _update_config_with_error_handling(self, data, dataset, zarr_out, s3_is_anon_access, living_lab):
        """Update the config file and handle errors."""
        try:
            self.update_config(data, dataset, zarr_out, self.config_file, s3_is_anon_access, living_lab)
            if dataset == "cems-glofas-seasonal":
                geojson_out = zarr_out.split('.zarr')[0]+'.geojson'
                self.update_config(data, dataset, geojson_out, self.config_file, s3_is_anon_access, living_lab)
        except Exception as e:
            logger.error(f"Error updating config: {e}")

    def split_var_to_variables(self, ds, data_var):
        """
        Splits the 'data_var' variable in an xarray dataset into separate variables for each 'number'.

        Parameters:
            ds (xarray.Dataset): Input dataset with 'data_var' variable having dimensions
                                (latitude, longitude, forecast_period, number).

        Returns:
            xarray.Dataset: New dataset with variables data_var_{number}.
        """
        # Ensure the 'data_var' variable exists
        if data_var not in ds:
            raise ValueError(f"Input dataset must contain the variable {data_var}.")

        # Ensure the 'number' dimension exists
        if 'number' not in ds[data_var].dims:
            raise ValueError(f"{data_var} variable must have a 'number' dimension.")

        # Extract the number coordinate
        number_values = ds['number'].values

        # Create a dictionary to hold the new variables
        new_data = ds.copy()

        for number in number_values:
            # Select the slice of data_var corresponding to the current number
            var_name = f"{data_var}_{number}"
            new_data[var_name] = ds[data_var].sel(number=number)

        # # Create a new dataset with the new variables
        new_data = new_data.drop_vars(data_var)
        new_data = new_data.drop_vars('number')
        return new_data

    def read_georgia_stations(self):
        stations_df = None
        try:
            stations_df = pd.read_csv('data/stations.csv')
        except FileNotFoundError:
            logger.debug("Error: 'stations.csv' not found. Please check the file path.")
        except pd.errors.EmptyDataError:
            logger.debug("Error: 'stations.csv' is empty.")
        except pd.errors.ParserError:
            logger.debug("Error: Unable to parse 'stations.csv'. Check the file format.")
        return stations_df
    
    def normalize_timestamp(self, value):
        """Convert large timestamps to seconds-based Unix epoch format."""
        try:
            if value > 10**13:  # Nanoseconds to seconds
                value /= 10**9
            elif value > 10**10:  # Milliseconds to seconds
                value /= 10**3
            return datetime.utcfromtimestamp(value).strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
            return None  # Handle invalid timestamps

    def create_geojson(self, ds):
        features = []

        stations_df = self.read_georgia_stations()
        for key, item in stations_df.iterrows():
            x_coord = item['x']
            y_coord = item['y']
            station_id = item['N']

            # Select data from xarray dataset based on nearest x, y
            selected_data = ds.sel(longitude=x_coord, latitude=y_coord, method='nearest')

            # Convert xarray dataset to dictionary with formatted datetime
            properties = {}
            for var in selected_data.variables:
                values = selected_data[var].values.tolist()
                
                # Flatten the list if it's a list of lists
                if isinstance(values, list) and any(isinstance(i, list) for i in values):
                    values = list(itertools.chain.from_iterable(values))

                properties[var] = values
                
                # Convert timestamps safely
                if var in ["forecast_period", "forecast_reference_time", "valid_time"]:
                    if isinstance(values, list):
                        properties[var] = [self.normalize_timestamp(v) for v in values if v is not None]
                    else:
                        properties[var] = self.normalize_timestamp(values)
                else:
                    properties[var] = values
            
            properties['id'] = int(station_id)

            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [x_coord, y_coord]
                },
                "properties": properties
            }

            features.append(feature)

        geojson = {
            "type": "FeatureCollection",
            "features": features
        }

        return json.dumps(geojson, indent=4)

    def __repr__(self):
        return f'<IngestorCDSProcessProcessor> {self.name}'
