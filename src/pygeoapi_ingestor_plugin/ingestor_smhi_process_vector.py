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
from .utils import check_running_jobs, read_config, write_config

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

LOGGER = logging.getLogger(__name__)

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
        self.config_file = os.environ.get(default='/pygeoapi/local.config.yml', key='PYGEOAPI_CONFIG_FILE')
        self.id = 'smhi-ingestor-vector-process'

    def execute(self, data):
        mimetype = 'application/json'

        # Extract and validate inputs
        file_out, issue_date, living_lab, s3_save = self._extract_and_validate_inputs(data)

        # Check running jobs and early return if jobs are running
        if self._check_running_jobs():
            return mimetype, {'message': 'There are running jobs, please try again later'}

        # Prepare file output path
        file_out = self._prepare_output_path(file_out, issue_date, s3_save)

        # Connect to FTP and retrieve files
        ftp_config = self._get_ftp_config()
        files = self._download_files_from_ftp(ftp_config, living_lab, data['data_dir'], issue_date)

        # Process files to extract features
        features = self._process_files(files, issue_date)

        # Create the feature collection and calculate bounding box and time range
        feature_collection, bbox, datetime_range = self._create_feature_collection_and_metadata(features)

        # Write the feature collection to the appropriate location (S3 or local)
        self._write_feature_collection(file_out, feature_collection)

        # Update and save the configuration
        self._update_config(living_lab, issue_date, file_out, bbox, datetime_range, s3_save)

        outputs = {'id': self.id, 'value': file_out}
        return mimetype, outputs
    
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
        file_out = data.get('file_out')
        issue_date = data.get('issue_date')
        living_lab = data.get('living_lab')
        s3_save = data.get('s3_save')
        self.token = data.get('token')

        if not issue_date:
            raise ProcessorExecuteError('Cannot process without an issue_date')
        if not data.get('data_dir'):
            raise ProcessorExecuteError('Cannot process without a data_dir')
        if not living_lab:
            raise ProcessorExecuteError('Cannot process without a living_lab')
        if not self.token or self.token != os.getenv("INT_API_TOKEN", "token"):
            raise ProcessorExecuteError('Identify yourself with a valid token!')

        return file_out, issue_date, living_lab, s3_save

    def _check_running_jobs(self):
        """Check if jobs are running."""
        return check_running_jobs(total_retries=10, time_out=30)

    def _prepare_output_path(self, file_out, issue_date, s3_save):
        """Prepare file output path, depending on S3 or local save."""
        if file_out and file_out.startswith('s3://'):
            s3 = s3fs.S3FileSystem()
            if s3.exists(file_out):
                raise ProcessorExecuteError(f'Path {file_out} already exists')
        elif s3_save:
            bucket_name = os.getenv("DEFAULT_BUCKET")
            remote_path = os.getenv("DEFAULT_REMOTE_DIR")
            file_out = f's3://{bucket_name}/{remote_path}dataset_smhi_{issue_date}.geojson'
        else:
            if not file_out:
                file_out = f'/pygeoapi/smhi_vector_data/dataset_smhi_{issue_date}.geojson'
        return file_out

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
        with FTP(ftp_config['url']) as ftp:
            ftp.login(user=ftp_config['user'], passwd=ftp_config['passwd'])
            ftp.cwd(remote_folder)
            return self.download_files_from_ftp(ftp, remote_folder)

    def _process_files(self, files, issue_date):
        """Process downloaded files and extract features."""
        features = []
        for file_nc in files:
            data, model, data_var = self._extract_data_from_netcdf(file_nc)
            geopandas_df = self._convert_to_geodataframe(data, data_var, model)
            features += self._extract_features_from_dataframe(geopandas_df, data_var, model)
        return features

    def _extract_data_from_netcdf(self, file_nc):
        """Extract data from NetCDF file."""
        data = self.read_netcdf(file_nc)
        data_var = [var for var in data.data_vars if var not in ['geo_x', 'geo_y', 'geo_z']][0]
        model = file_nc.split(f'{data_var}_')[1].split('.')[0]
        data[f"{data_var}_{model}"] = data[data_var]
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

    def _extract_features_from_dataframe(self, gdf, data_var, model):
        """Extract features from GeoDataFrame."""
        features = []
        for id in gdf.id.unique():
            df = gdf[gdf.id == id]
            time_series = df['time'].tolist()
            var_series = df[f"{data_var}_{model}"].tolist()
            feature = self._get_or_create_feature(features, id, df, data_var, model, time_series, var_series)
            if feature not in features:
                features.append(feature)
        return features

    def _get_or_create_feature(self, features, id, df, data_var, model, time_series, var_series):
        """Get an existing feature by ID or create a new one."""
        for f in features:
            if f.get("properties", {}).get("id") == id:
                f["properties"][f"{data_var}_{model}"] = var_series
                return f

        new_feature = Feature(
            geometry=Point((df.geo_x.iloc[0], df.geo_y.iloc[0])),
            properties={"id": id, "time": time_series, f"{data_var}_{model}": var_series}
        )
        return new_feature

    def _create_feature_collection_and_metadata(self, features):
        """Create feature collection and calculate bounding box and time range."""
        feature_collection = FeatureCollection(features)
        bbox = self._calculate_bbox(features)
        datetime_range = self._calculate_time_range(features)
        return feature_collection, bbox, datetime_range

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
        
        config = read_config(self.config_file)
        resource_key = f'{living_lab}_seasonal_forecast_{issue_date}'
        # Convert all values to float32
        bbox_float = [float(val) for val in bbox]
        config['resources'][resource_key] = {
            'type': 'collection',
            'title': resource_key,
            'description': f'SMHI Discharge data of {living_lab}',
            'keywords': [living_lab, 'country'],
            'extents': {'spatial': {'bbox': bbox_float, 'crs': 'http://www.opengis.net/def/crs/OGC/1.3/CRS84'},
                        'temporal': {'begin': datetime_range[0], 'end': datetime_range[1]}},
            'providers': [{'type': 'feature', 'name': 'GeoJSON', 'data': file_out, 'id_field': 'id'}]
        }
        
        if s3_save:
            config['resources'][resource_key]['providers'][0]['options'] = {'s3': {'anon': True, 'requester_pays': False}}
        write_config(self.config_file, config)
    
    def __repr__(self):
        return f'<IngestorSMHIVectorProcessProcessor> {self.name}'
