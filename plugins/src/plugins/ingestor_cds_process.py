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
import datetime
import xarray as xr
import json
import tempfile
from dotenv import load_dotenv, find_dotenv
import cdsapi
import zipfile
import cfgrib
import ecmwflibs


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



def download_files_from_ftp(ftp, folder):
    # ftp.cwd(folder)
    files = ftp.nlst()
    nc_files = []
    for file in files:
        if file.endswith('.nc'):
            local_filename = os.path.join(f"./seasonal_forecast/{folder}", file)
            nc_files.append(local_filename)
            if not os.path.exists(local_filename):

                if not os.path.exists(f"./seasonal_forecast/{folder}"):
                    os.makedirs(f"./seasonal_forecast/{folder}")
                with open(local_filename, 'wb') as f:
                    ftp.retrbinary('RETR ' + file, f.write)
                # print(f"Downloaded: {local_filename}")
            else:
                # print(f"File already exists: {local_filename}")
                pass
    ftp.cwd("..")
    return nc_files

# Function to read geometry and bbox from NetCDF file
def read_netcdf(file_path):
    nc_file = fsspec.open(file_path,anon=True)
    nc = xr.open_dataset(nc_file.open())  # Dataset(file_path, 'r')
    # Extract geometry and bbox from the NetCDF file

    return nc


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
        file_out = data.get('file_out', os.path.join(f"{tempfile.gettempdir()}",f"copernicus_data_{str(int(datetime.datetime.now().timestamp()))}.nc"))
        zarr_out = data.get('zarr_out')
        engine = data.get('engine', 'h5netcdf')


        if dataset is None:
            raise ProcessorExecuteError('Cannot process without a dataset')
        if query is None:
            raise ProcessorExecuteError('Cannot process without a query')
        
        s3 = s3fs.S3FileSystem(anon=True)
        if zarr_out:
            remote_url = zarr_out
            # Check if the path already exists
            if s3.exists(remote_url):
                raise ProcessorExecuteError(f'Path {remote_url} already exists')
        else:    
            bucket_name = os.environ.get("DEFAULT_BUCKET")
            remote_path = os.environ.get("DEFAULT_REMOTE_DIR")
            remote_url = f's3://{bucket_name}/{remote_path}dataset_cds_{int(datetime.datetime.now().timestamp())}.zarr'

        # print("Dataset", dataset)
        # print("Query", query)
        # print(f"Fileout: {file_out}")
        # print("zarr_out",zarr_out)
        # print("engine", engine)

        URL = 'https://cds.climate.copernicus.eu/api/v2'
        KEY = os.environ.get('CDSAPI_KEY')
        # DATADIR = './test_data/seasonal'
        # {
        #     'format': 'grib',
        #     'originating_centre': 'ecmwf',
        #     'system': '5',
        #     'variable': varname,
        #     'product_type': 'monthly_mean',
        #     'year': years,
        #     'month': month,
        #     'leadtime_month': leadtime_month,
        # }

        c = cdsapi.Client(url=URL, key=KEY)

        # Hindcast data request
        c.retrieve(
            dataset,    # 'seasonal-monthly-single-levels',
            query,
            file_out)

        if file_out.endswith('.grib'):
            data = xr.open_dataset(file_out,engine="cfgrib")

        elif file_out.endswith('.netcdf4.zip'):
            # Unzip the file

            with zipfile.ZipFile(file_out, 'r') as zip_ref:
                local_file = f"{file_out.split('.netcdf4.zip')[0]}.nc"
                zip_ref.extractall(local_file)

            data = xr.open_dataset(f"{local_file}/data.nc", engine=engine)
        
        for var in data.data_vars:
            data[var] = data[var].expand_dims(dim='time')
        data.attrs['long_name'] = dataset

        store= s3fs.S3Map(root=remote_url, s3=s3, check=False)

        data.to_zarr(store=store,
                            consolidated=True,
                
                    mode='w')
        # print(data)

        # get min/max values for longitude, latitude and time
        min_x = float(data.coords['longitude'].min().values)
        max_x = float(data.coords['longitude'].max().values)
        min_y = float(data.coords['latitude'].min().values)
        max_y = float(data.coords['latitude'].max().values)

        min_time = data.coords['time'].min().values
        max_time = data.coords['time'].max().values
        
        # convert np.datetime64 to datetime object 
        datetime_max = datetime.datetime.fromtimestamp(max_time.tolist()/1e9,tz=datetime.timezone.utc)
        datetime_min = datetime.datetime.fromtimestamp(min_time.tolist()/1e9,tz=datetime.timezone.utc)

        with open('/pygeoapi/local.config.yml', 'r') as file:
            config = yaml.safe_load(file)

        config['resources'][dataset] = {
            'type': 'collection',
            'title': dataset,
            'description': 'CDS Discharge data of Georgia',
            'keywords': ['Georgia', 'country'],
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
                    'data': remote_url,
                    'x_field': 'longitude',
                    'y_field': 'latitude',
                    'time_field': 'time',
                    'format': {'name': 'zarr', 'mimetype': 'application/zip'},
                    'options': {
                        's3': {'anon': True, 'requester_pays': False}
                    }
                }
            ]
        }

        with  open('/pygeoapi/local.config.yml', 'w') as outfile:
            yaml.dump(config, outfile, default_flow_style=False)


        outputs = {
            'id': 'ingestor-process',
            'value': remote_url
        }
        return mimetype, outputs

    def __repr__(self):
        return f'<IngestorCDSProcessProcessor> {self.name}'

