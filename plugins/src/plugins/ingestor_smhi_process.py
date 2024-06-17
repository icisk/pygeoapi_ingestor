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
import time
import yaml
import fsspec
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
import os
import s3fs
import datetime
import xarray as xr
import logging
import sys

logging.basicConfig(
    format="[%(levelname)s] %(asctime)s %(message)s",
    level=logging.DEBUG,
    stream=sys.stdout)
logger = logging.getLogger(__name__)


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
        'issue_date': {
            'title': 'Issue Date',
            'description': 'The issue date of the forecast',
            'schema': {
                'type': 'string'
            }
        },
        'data_dir': {
            'title': 'Data Directory',
            'description': 'The directory where the data is stored',
            'schema': {
                'type': 'string'
            }
        },
        'living_lab': {
            'title': 'Living Lab',
            'description': 'The living lab for which the data is fetched',
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
            "issue_date": "202404",
            "data_dir": "seasonal_forecast",
            "living_lab": "georgia"
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
                logger.debug(f"Downloaded: {local_filename}")
            else:
                logger.debug(f"File already exists: {local_filename}")
                pass
    ftp.cwd("..")
    return nc_files

# Function to read geometry and bbox from NetCDF file
def read_netcdf(file_path):
    nc_file = fsspec.open(file_path,anon=True)
    nc = xr.open_dataset(nc_file.open())  # Dataset(file_path, 'r')
    # Extract geometry and bbox from the NetCDF file

    return nc


class IngestorSMHIProcessProcessor(BaseProcessor):
    """
    Ingestor Processor example
    """

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition

        :returns: pygeoapi.process.ingestor_process.IngestorSMHIProcessProcessor
        """

        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):

        mimetype = 'application/json'

        data_dir = data.get('data_dir')
        issue_date = data.get('issue_date')
        living_lab = data.get('living_lab')
        zarr_out = data.get('zarr_out')
        ftp_config= {
            "url": os.environ.get("FTP_HOST"),
            "folder": os.environ.get("FTP_DIR"),
            "user": os.environ.get("FTP_USER"),
            "passwd": os.environ.get("FTP_PASS")
        }

        if issue_date is None:
            raise ProcessorExecuteError('Cannot process without a issue_date')
        if data_dir is None:
            raise ProcessorExecuteError('Cannot process without a data_dir')
        if living_lab is None:
            raise ProcessorExecuteError('Cannot process without a living_lab')

        s3 = s3fs.S3FileSystem(anon=True)
        if zarr_out:
            remote_url = zarr_out
            # Check if the path already exists
            if s3.exists(remote_url):
                raise ProcessorExecuteError(f'Path {remote_url} already exists')
        else:
            bucket_name = os.environ.get("DEFAULT_BUCKET")
            remote_path = os.environ.get("DEFAULT_REMOTE_DIR")
            remote_url = f's3://{bucket_name}/{remote_path}dataset_smhi_{int(datetime.datetime.now().timestamp())}.zarr'

        data_array = []

        # Connect to FTP server
        ftp = FTP(ftp_config['url'])
        ftp.login(user=ftp_config['user'], passwd=ftp_config['passwd'])

        remote_folder = f"{ftp_config['folder']}/{living_lab}/{data_dir}/{issue_date}"
        local_folder = f"./{data_dir}/{issue_date}"

        with FTP(ftp_config['url']) as ftp:
            ftp.login(ftp_config['user'], ftp_config['passwd'])
            ftp.cwd(remote_folder)

            files = download_files_from_ftp(ftp, remote_folder)

        for file_nc in files:
            file_nc_path = f"{local_folder}/{file_nc}"
            data = read_netcdf(file_nc)

            # extract data variables key
            data_var = [var for var in data.data_vars if var not in ['geo_x', 'geo_y', 'geo_z']][0]
            model = file_nc_path.split(f'{data_var}_')[1].split('.')[0]

            if data_var in data:
                data[f"{data_var}_{model}"] = data[data_var]
                data[f"{data_var}_{model}"].attrs['long_name'] = f"{data_var}_{model}"
                # drop the original data variable
                data = data.drop_vars(data_var)
            data_array.append(data)

        data = xr.merge(data_array)

        df = data.to_dataframe()

        # Reset the index to convert the MultiIndex into columns
        df_reset = df.reset_index()
        df_reset = df_reset.drop(columns=['geo_z'])

        # Set 'geo_x' and 'geo_y' as part of the new index and drop 'id'
        df_reindexed = df_reset.set_index(['geo_x', 'geo_y', 'id','time'])

        df_reindexed = df_reindexed.stack().dropna().unstack()

        # drop id from the index
        df_reindexed = df_reindexed.reset_index().set_index(['geo_x', 'geo_y', 'time'])

        # convert the dataframe to xarray dataset
        data = xr.Dataset.from_dataframe(df_reindexed)

        # Select the first time slice
        id_var = data['id'].isel(time=0)

        # Drop the time dimension
        id_var = id_var.drop('time')

        # Assign the modified variable back to the dataset
        data['id'] = id_var


        store= s3fs.S3Map(root=remote_url, s3=s3, check=False)

        data.attrs['long_name'] = "seasonal_forecast"
        for var in data.variables:
            data[var].attrs['long_name'] = var

        data.to_zarr(store=store,
                            consolidated=True,
                            mode='w')

        # get min/max values for geo_x, geo_y and time
        min_x = float(data['geo_x'].min().values)
        max_x = float(data['geo_x'].max().values)
        min_y = float(data['geo_y'].min().values)
        max_y = float(data['geo_y'].max().values)

        min_time = data['time'].min().values
        max_time = data['time'].max().values

        # convert np.datetime64 to datetime object
        datetime_max = datetime.datetime.fromtimestamp(max_time.tolist()/1e9,tz=datetime.timezone.utc)
        datetime_min = datetime.datetime.fromtimestamp(min_time.tolist()/1e9,tz=datetime.timezone.utc)

        with open('/pygeoapi/local.config.yml', 'r') as file:
            config = yaml.safe_load(file)

        config['resources'][f'georgia_seasonal_forecast_{issue_date}'] = {
            'type': 'collection',
            'title': f'georgia_seasonal_forecast_{issue_date}',
            'description': 'SMHI Discharge data of Georgia',
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
                    'x_field': 'geo_x',
                    'y_field': 'geo_y',
                    'time_field': 'time',
                    'format': {'name': 'zarr', 'mimetype': 'application/zip'},
                    'options': {
                        's3': {'anon': True, 'requester_pays': False}
                    }
                }
            ]
        }

        logger.debug("***********************************")
        logger.debug(config['resources'][f'georgia_seasonal_forecast_{issue_date}'])
        logger.debug("***********************************")

        with  open('/pygeoapi/local.config.yml', 'w') as outfile:
            yaml.dump(config, outfile, default_flow_style=False)


        outputs = {
            'id': 'ingestor-process',
            'value': remote_url
        }
        return mimetype, outputs

    def __repr__(self):
        return f'<IngestorSMHIProcessProcessor> {self.name}'
