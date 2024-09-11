from ftplib import FTP
import logging, time
import yaml
import fsspec
import os
import s3fs
from datetime import datetime
import xarray as xr
from geojson import Feature, Point, FeatureCollection
import geopandas as gpd
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
import xarray as xr
from .utils import read_config, write_config

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
            "issue_date": "202404",
            "data_dir": "seasonal_forecast",
            "living_lab": "georgia",
            "token": "ABC123XYZ666"
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


    def execute(self, data):

        mimetype = 'application/json'

        data_dir = data.get('data_dir')
        issue_date = data.get('issue_date')
        living_lab = data.get('living_lab')
        file_out = data.get('file_out')
        s3_save = data.get('s3_save')
        ftp_config= {
            "url": os.environ.get("FTP_HOST"),
            "folder": os.environ.get("FTP_DIR"),
            "user": os.environ.get("FTP_USER"),
            "passwd": os.environ.get("FTP_PASS")
        }
        self.token = data.get('token')

        if issue_date is None:
            raise ProcessorExecuteError('Cannot process without a issue_date')
        if data_dir is None:
            raise ProcessorExecuteError('Cannot process without a data_dir')
        if living_lab is None:
            raise ProcessorExecuteError('Cannot process without a living_lab')
        if self.token is None:
            raise ProcessorExecuteError('Identify yourself with valid token!')
        
        if self.token is not os.getenv("INT_API_TOKEN", "token"):
            LOGGER.error("WRONG INTERNAL API TOKEN")
            raise ProcessorExecuteError('ACCES DENIED wrong token')

        if file_out and file_out.startswith('s3://'):
            s3_save = True
            s3 = s3fs.S3FileSystem(anon=True)
            # Check if the path already exists
            if s3.exists(file_out):
                raise ProcessorExecuteError(f'Path {file_out} already exists')
        else:
            if s3_save:
                bucket_name = os.environ.get("DEFAULT_BUCKET")
                remote_path = os.environ.get("DEFAULT_REMOTE_DIR")
                file_out = f's3://{bucket_name}/{remote_path}dataset_smhi_{issue_date}.geojson'
            else:
                if not file_out:
                    #FIXME use env PYGEOAPI_HOME
                    file_out = f'/pygeoapi/smhi_vector_data/dataset_smhi_{issue_date}.geojson'

        features = []

        # Connect to FTP server
        ftp = FTP(ftp_config['url'])
        ftp.login(user=ftp_config['user'], passwd=ftp_config['passwd'])

        remote_folder = f"{ftp_config['folder']}/{living_lab}/{data_dir}/{issue_date}"

        with FTP(ftp_config['url']) as ftp:
            ftp.login(ftp_config['user'], ftp_config['passwd'])
            ftp.cwd(remote_folder)

            files = download_files_from_ftp(ftp, remote_folder)

        for file_nc in files:
            file_nc_path = f"{file_nc}"
            data = read_netcdf(file_nc_path)

            # extract data variables key
            data_var = [var for var in data.data_vars if var not in ['geo_x', 'geo_y', 'geo_z']][0]
            model = file_nc_path.split(f'{data_var}_')[1].split('.')[0]

            if data_var in data:
                data[f"{data_var}_{model}"] = data[data_var]
                data[f"{data_var}_{model}"].attrs['long_name'] = f"{data_var}_{model}"
                # drop the original data variable
                data = data.drop_vars(data_var)

            dataframe = data.to_dataframe().reset_index()
            geopandas_df = gpd.GeoDataFrame(dataframe, geometry=gpd.points_from_xy(dataframe.geo_x, dataframe.geo_y))
            geopandas_df['time'] = geopandas_df['time'].dt.strftime('%Y-%m-%d %H:%M:%S')
            # Convert columns to float64 if necessary
            geopandas_df = geopandas_df.astype({'geo_x': 'float64',
                                                'geo_y': 'float64',
                                                'geo_z': 'float64',
                                                f"{data_var}_{model}": 'float64',
                                                'id': 'str'})

            # create a feature for each id
            # fore each unique id in the dataframe
            for id in geopandas_df.id.unique():
                # filter the dataframe by id
                df = geopandas_df[geopandas_df.id == id]
                # print(df)
                time_serie = []
                var_serie = []
                for index, row in df.iterrows():
                    time_serie.append(row['time'])
                    var_serie.append(row[f"{data_var}_{model}"])
                # create a feature

                # if no feature with the same id exists in features inside features list, create a new feature and append it
                # else, append the time and var series to the existing feature
                # if features is empty, create a new feature and append it
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

        feature_collection = FeatureCollection(features)

        # get min and max values for bbox from features
        min_x = float(min([f['geometry']['coordinates'][0] for f in features]))
        min_y = float(min([f['geometry']['coordinates'][1] for f in features]))
        max_x = float(max([f['geometry']['coordinates'][0] for f in features]))
        max_y = float(max([f['geometry']['coordinates'][1] for f in features]))

        # get datetime min and max values
        datetime_objects = [datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S") for f in features for date_str in f['properties']['time']]
        # Find the minimum and maximum datetime
        min_time = min(datetime_objects)
        max_time = max(datetime_objects)
        datetime_min = min_time
        datetime_max = max_time

        with open(file_out, 'w') as f:
            f.write(str(feature_collection))

        config = read_config(self.config_file)

        config['resources'][f'{living_lab}_seasonal_forecast_{issue_date}'] = {
            'type': 'collection',
            'title': f'{living_lab}_seasonal_forecast_{issue_date}',
            'description': f'SMHI Discharge data of {living_lab}',
            'keywords': [living_lab, 'country'],
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
                    'type': 'feature',
                    'name': 'GeoJSON',
                    'data': file_out,
                    'id_field': 'id',
                    # 'options': {
                    #     's3': {'anon': True, 'requester_pays': False}
                    # }
                }
            ]
        }

        if s3_save:
            config['resources'][f'{living_lab}_seasonal_forecast_{issue_date}']['providers'][0]['options'] = {
                's3': {'anon': True, 'requester_pays': False}
            }

        write_config(self.config_file, config)
        
        outputs = {
            'id': 'smhi-ingestor-process',
            'value': file_out
        }
        return mimetype, outputs

    def __repr__(self):
        return f'<IngestorSMHIVectorProcessProcessor> {self.name}'
