from ftplib import FTP
import logging
import fsspec
import os
import numpy as np
import s3fs
from datetime import datetime
import xarray as xr
from geojson import Feature, Point, FeatureCollection
import geopandas as gpd
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
from .utils import read_config, write_config
from filelock import FileLock
import pandas as pd
import time
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
    'id': 'smhi-ingestor-process',
    'title': {
        'en': 'SMHI Ingestor Process',
    },
    'description': {
        'en': 'Ingestor process for fetching SMHI data from FTP server and creating a GeoJSON file'},
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
        'file_out': {
            'title': 'Geojson Output',
            'description': 'The URL of the geojson file in the S3 bucket or local path',
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
            'description': 'The URL of the geojson file in the S3 bucket or local path',
            'schema': {
                'type': 'string'
            }
        }
    },
    'example': {
        "inputs": {
            "issue_date": "202409",
            "data_dir": "seasonal_forecast",
            "living_lab": "georgia",
            "token": "ABC123XYZ666",
            "file_out": "s3://saferplaces.co/test/icisk/test_sf_georgia_202409.geojson"
        }
    }
}


class IngestorSMHIVectorProcessProcessor(BaseProcessor):
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
        self.id = 'smhi-ingestor-vector-process'

    def execute(self, data):

        mimetype = 'application/json'

        # Extract and validate inputs
        file_out, issue_date, living_lab, s3_save, data_dir = self._extract_and_validate_inputs(data)

        # Prepare output path
        msg = self._prepare_output_path(file_out, issue_date, s3_save, living_lab)
        if msg:
            return mimetype, {'id': self.id, 'value': msg}
        
        # Connect to FTP and retrieve files
        ftp_config = self._get_ftp_config()

        files = self._download_files_from_ftp(ftp_config, living_lab, data_dir, issue_date)
        if files == []:
            return mimetype, {'id': self.id, 'value': 'No files found in FTP server'}

        features = []
        for file_nc in files:

            data, model, data_var = self._extract_data_from_netcdf(file_nc)

            geopandas_df = self._convert_to_geodataframe(data, data_var, model)
            
            # create a feature for each id
            # fore each unique id in the dataframe
            for id in geopandas_df.id.unique():
                # filter the dataframe by id
                df = geopandas_df[geopandas_df.id == id]
                time_serie = []
                var_serie = []
                for index, row in df.iterrows():
                    time_serie.append(row['time'])
                    var_serie.append(row[f"{data_var}_{model}"])

                # if no feature with the same id exists in features inside features list, create a new feature and append it
                # else, append the time and var series to the existing feature
                # if features is empty, create a new feature and append it
                features = self._process_feature(features, id, df, data_var, model, time_serie, var_serie)

        feature_collection = FeatureCollection(features)

        # get min and max values for bbox from features
        bbox = self._calculate_bbox(features)
        
        # get datetime min and max values
        datetime_range = self._calculate_time_range(features)

        # Write the feature collection to the appropriate location (S3 or local)
        self._write_feature_collection(file_out, feature_collection)

        self._update_config(living_lab, issue_date, file_out, bbox, datetime_range, s3_save)
        
        outputs = {
            'id': self.id,
            'value': file_out
        }
        return mimetype, outputs

    def _process_feature(self, features, id, df, data_var, model, time_serie, var_serie):
        if len(features) == 0:
            new_feature = Feature(geometry=Point((df.geo_x.iloc[0], df.geo_y.iloc[0])),
                                properties={"id": id,
                                            "time": time_serie,
                                            f"{data_var}_{model}": var_serie})
            features.append(new_feature)
        else:
            found = False
            sel_feature = None
            for f in features:

                if f.get("properties", {}).get("id") == id:
                    found = True
                    sel_feature = f
                    break

            if found:
                sel_feature["properties"][f"{data_var}_{model}"] = var_serie
            else:

                new_feature = Feature(geometry=Point((df.geo_x.iloc[0], df.geo_y.iloc[0])),
                                    properties={"id": id,
                                                "time": time_serie,
                                                f"{data_var}_{model}": var_serie})
                features.append(new_feature)
        return features
    
    def download_files_from_ftp(self, ftp, folder):
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
        ftp.cwd("..")
        return nc_files

    # Function to read geometry and bbox from NetCDF file
    def read_netcdf(self, file_path):
        nc_file = fsspec.open(file_path,anon=True)
        nc = xr.open_dataset(nc_file.open())  # Dataset(file_path, 'r')
        # Extract geometry and bbox from the NetCDF file

        return nc

    def _extract_and_validate_inputs(self, data):
        """Extract inputs and raise errors for missing values."""
        
        cron_invocation = data.get('cron_invocation')
        
        living_lab = data.get('living_lab')
        s3_save = data.get('s3_save')
        self.token = data.get('token')
        data_dir = data.get('data_dir')

        if cron_invocation:
            current_issue_date = time.strftime("%Y%m")
            issue_date = current_issue_date
            logger.info(f'Cron invocation requested. Current issue date: {current_issue_date}')
        else:
            issue_date = data.get('issue_date')

        file_out = f"{data.get('file_out').split('.geojson')[0]}{issue_date}.geojson" # f"s3://52n-i-cisk/data-ingestor/smhi_seasonal_forecast_{living_lab}_{time.strftime('%Y%m')}.geojson"

        if not issue_date:
            raise ProcessorExecuteError('Cannot process without an issue_date')
        if not data.get('data_dir'):
            raise ProcessorExecuteError('Cannot process without a data_dir')
        if not living_lab:
            raise ProcessorExecuteError('Cannot process without a living_lab')
        if not self.token or self.token != os.getenv("INT_API_TOKEN", "token"):
            raise ProcessorExecuteError('Identify yourself with a valid token!')

        return file_out, issue_date, living_lab, s3_save, data_dir

    def _prepare_output_path(self, file_out, issue_date, s3_save, living_lab):
        """Prepare file output path, depending on S3 or local save."""
        msg = None
        if file_out and file_out.startswith('s3://'):
            s3 = s3fs.S3FileSystem()
            if s3.exists(file_out):
                resource_key = f'{living_lab}_seasonal_forecast_{issue_date}'
                if resource_key in read_config(self.config_file)['resources']:
                    logger.info(f'Path {file_out} already exists in bucket and config')
                    msg = f"Path {file_out} already exists in bucket and config"
                # TODO: Get the data from the bucket and update the config
                # else:
                #     logger.info(f'Path {file_out} already exists in bucket but not in config')
                #     # Process files to extract features
                #     data = gpd.read_file(file_out)

                #     bbox = data.total_bounds
                #     time_list = data['time']
                #     datetime_list = pd.to_datetime(time_list[0])
                #     datetime_range = (datetime_list.min(), datetime_list.max())
                #     # Update and save the configuration
                #     self._update_config(living_lab, issue_date, file_out, bbox, datetime_range, s3_save)
                #     msg = f"Path {file_out} already exists updates config at '{self.config_file}'"
        return msg

    def _get_ftp_config(self):
        """Get FTP configuration from environment variables."""
        return {
            "url": os.getenv("FTP_HOST"),
            "folder": os.getenv("FTP_DIR"),
            "user": os.getenv("FTP_USER"),
            "passwd": os.getenv("FTP_PASS")
        }

    def _download_files_from_ftp(self, ftp_config, living_lab, data_dir, issue_date):
        """Connect to FTP and download files."""
        remote_folder = f"{ftp_config['folder']}/{living_lab}/{data_dir}/{issue_date}"
        try:
            with FTP(ftp_config['url']) as ftp:
                ftp.login(user=ftp_config['user'], passwd=ftp_config['passwd'])
                ftp.cwd(remote_folder)
                return self.download_files_from_ftp(ftp, remote_folder)
        except Exception as e:
            logger.error(f"Error downloading files from FTP: {e}")
            return []

    def _extract_data_from_netcdf(self, file_nc):
        """Extract data from NetCDF file."""
        data = self.read_netcdf(file_nc)
        data_var = [var for var in data.data_vars if var not in ['geo_x', 'geo_y', 'geo_z']][0]
        model = file_nc.split(f'{data_var}_')[1].split('.')[0]
        data[f"{data_var}_{model}"] = data[data_var]
        data = data.drop_vars(data_var)
        
        if data_var in data:
            data[f"{data_var}_{model}"] = data[data_var]
            data[f"{data_var}_{model}"].attrs['long_name'] = f"{data_var}_{model}"
            # drop the original data variable
            data = data.drop_vars(data_var)

        return data, model, data_var
    
    def _convert_to_geodataframe(self, data, data_var, model):
        """Convert data to a GeoDataFrame."""
        dataframe = data.to_dataframe().reset_index()
        gdf = gpd.GeoDataFrame(dataframe, geometry=gpd.points_from_xy(dataframe.geo_x, dataframe.geo_y))
        gdf['time'] = gdf['time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        gdf = gdf.astype({
            'geo_x': 'float64', 'geo_y': 'float64', 'geo_z': 'float64',
            f"{data_var}_{model}": 'float64', 'id': 'str'
        })
        return gdf
    
    def _calculate_bbox(self, features):
        """Calculate bounding box for the features."""
        min_x = min([f['geometry']['coordinates'][0] for f in features])
        min_y = min([f['geometry']['coordinates'][1] for f in features])
        max_x = max([f['geometry']['coordinates'][0] for f in features])
        max_y = max([f['geometry']['coordinates'][1] for f in features])
        return [min_x, min_y, max_x, max_y]

    def _calculate_time_range(self, features):
        """Calculate the min and max datetime range from features."""
        datetime_objects = [datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S") for f in features for date_str in f['properties']['time']]
        return min(datetime_objects), max(datetime_objects)

    def _write_feature_collection(self, file_out, feature_collection):
        """Write the feature collection to S3 or local file."""
        if file_out.startswith('s3://'):
            s3 = s3fs.S3FileSystem()
            with s3.open(file_out, 'w') as f:
                f.write(str(feature_collection))
        else:
            with open(file_out, 'w') as f:
                f.write(str(feature_collection))

    def _update_config(self, living_lab, issue_date, file_out, bbox, datetime_range, s3_save):
        """Update and write configuration."""
        # THIS MUST BE THE SAME IN ALL PROCESSES UPDATING THE SERV CONFIG
        lock = FileLock(f"{self.config_file}.lock", thread_local=False)

        with lock:
            config = read_config(self.config_file)
            resource_key = f'{living_lab}_seasonal_forecast_{issue_date}'
            # Convert all values to float32
            bbox_float = [float(val) for val in bbox]
            config['resources'][resource_key] = {
                'type': 'collection',
                'title': resource_key,
                'description': f'SMHI Discharge data of {living_lab}',
                'keywords': [
                    living_lab,
                    'country',
                    'discharge',
                    'forecast',
                    'smhi',
                    'seasonal'
                ],
                'extents': {
                    'spatial': {
                        'bbox': bbox_float,
                        'crs': 'http://www.opengis.net/def/crs/OGC/1.3/CRS84'
                    },
                    'temporal': {
                        'begin': datetime_range[0],
                        'end': datetime_range[1]
                    }
                },
                'providers': [{
                    'type': 'feature',
                    'name': 'S3GeoJSONProvider.S3GeoJSONProvider',
                    'data': file_out,
                    'id_field': 'id'
                }]
            }

            if s3_save:
                config['resources'][resource_key]['providers'][0]['options'] = {
                    's3': {
                        'anon': True,
                        'requester_pays': False
                    }
                }
            write_config(self.config_file, config)

    def __repr__(self):
        return f'<IngestorSMHIVectorProcessProcessor> {self.name}'
