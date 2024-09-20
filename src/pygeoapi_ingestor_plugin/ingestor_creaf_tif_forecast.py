from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from filelock import FileLock

import fsspec
import os
import s3fs
import yaml

import tifffile as tiff
import numpy as np
import xarray as xr

from osgeo import gdal

import logging
from dotenv import load_dotenv, find_dotenv

from .utils import download_source, cleanup_data_temp

LOGGER = logging.getLogger(__name__)

load_dotenv(find_dotenv())

PROCESS_METADATA = {
    'version': '0.2.0',
    'id': 'creaf_forecast_ingestor',
    'title': {
        'en': 'creaf_forecast',
    },
    'description': {
        'en': 'joins tiff-data and saves it to zarr format and uploads it to s3 bucket'},
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
        'data_source': {
            'title': 'data_source',
            'description': 'URL to the data, e.g. https://example.com/my-data.zip',
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
            "data_source": "file:///data/creaf/precip_forecast",
            "zarr_out": "s3://example/target/bucket.zarr",
            "token": "ABC123XYZ666"
        }
    }
}


def get_pixel_centroids(file_path):
    ds = gdal.Open(file_path)
    gt = ds.GetGeoTransform()
    x_size, y_size = ds.RasterXSize, ds.RasterYSize

    x_centroids = [gt[0] + gt[1] * (x + 0.5) for x in range(x_size)]
    y_centroids = [gt[3] + gt[5] * (y + 0.5) for y in range(y_size)]

    return x_centroids, y_centroids

def tifs_to_ds(path):
    files = [os.path.join(path, f) for f in sorted(os.listdir(path)) if f.endswith('.tif') or f.endswith('.tiff')]
    # naming and metadata
    file_names = [os.path.splitext(f)[0] for f in sorted(os.listdir(path))]
    variables = sorted(set([name.split("_")[3] for name in file_names]))
    files_per_var = [[f for f in files if os.path.basename(f).split("_")[3] == var] for var in variables]
    time = sorted(set([np.datetime64(f'{parts[1]}-{int(parts[0]):02}-01') for fn in file_names for parts in [fn.split("_")[1:4]]]))
    x, y = get_pixel_centroids(files[0])
    # xarray creation
    da_list = []
    for i, group in enumerate(files_per_var):
            arrays = [tiff.imread(file) for file in group]
            stacked = np.stack(arrays, axis=0)
            da = xr.DataArray(stacked,
                            dims=['time', 'latitude', 'longitude'],
                            name=variables[i])
            da_list.append(da)

    ds = xr.Dataset({variables[i]: (['time', 'latitude', 'longitude'], da_list[i].data) for i in range(len(da_list))},
                    coords={'time': time,
                            'latitude': y,
                            'longitude': x},
                    attrs={'long_name': 'precip',
                            'units': 'mm'})


    for var in variables:
        if var == 'UNCERTAINTY':
            ds[var].attrs = {'long_name': 'quality of data',
                             'units': 'categorical'}
        else:
            ds[var].attrs = ds.attrs

    return ds


def tifs_to_da(path):
    # file paths
    files = [os.path.join(path, f) for f in sorted(os.listdir(path)) if f.endswith('.tif') or f.endswith('.tiff')]
    # naming and metadata
    file_names = [os.path.splitext(f)[0] for f in sorted(os.listdir(path))]
    info = [f"{int(parts[0]):02}_{parts[1]}_{parts[2]}" for fn in file_names for parts in [fn.split("_")[1:4]]]
    time = [np.datetime64(f'{parts[1]}-{parts[0]}-01T00:{i:02}') for i, date in enumerate(info) for parts in [date.split("_")]]
    x, y = get_pixel_centroids(files[0])
    # xarray creation
    arrays = [tiff.imread(file) for file in files]
    stacked = np.stack(arrays, axis=0)
    da = xr.DataArray(stacked,
                      dims=['time', 'latitude', 'longitude'],
                      coords={'time': time, 'latitude': y, 'longitude': x},
                      attrs={'info': info,
                             'long_name': 'long name info',
                             'units': 'units info'},
                      name='data')

    return da



class IngestorCREAFFORECASTProcessProcessor(BaseProcessor):
    """
    Ingestor Processor example
    """

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition

        :returns: pygeoapi.process.ingestor_process.IngestorCREAFFORECASTProcessProcessor
        """

        super().__init__(processor_def, PROCESS_METADATA)
        self.config_file = os.environ.get(default='/pygeoapi/serv-config/local.config.yml', key='PYGEOAPI_SERV_CONFIG')
        self.title = 'creaf_forecast'
        self.otc_key = os.environ.get(key='FSSPEC_S3_KEY')
        self.otc_secret = os.environ.get(key='FSSPEC_S3_SECRET')
        self.otc_endpoint = os.environ.get(key='FSSPEC_S3_ENDPOINT_URL')
        self.alternate_root = None
        self.data_path = None
        self.zarr_out = None

    def read_config(self):
        with open(self.config_file, 'r') as file:
            LOGGER.debug("read config")
            return(yaml.safe_load(file))

    def write_config(self, new_config):
        with  open(self.config_file, 'w') as outfile:
            yaml.dump(new_config, outfile, default_flow_style=False)
        LOGGER.debug("updated config")

    def get_data_from_cloud(self):
        mapper = fsspec.get_mapper(self.zarr_out,
                       alternate_root=self.alternate_root,
                       endpoint_url = self.otc_endpoint,
                       key=self.otc_key,
                       secret=self.otc_secret,
                       )
        return xr.open_zarr(mapper)


    def update_config(self):
        da = self.get_data_from_cloud()
        min_x = float(da.longitude.values.min())
        max_x = float(da.longitude.values.max())
        min_y = float(da.latitude.values.min())
        max_y = float(da.latitude.values.max())

        # THIS MUST BE THE SAME IN ALL PROCESSES UPDATING THE SERV CONFIG
        lock = FileLock(f"{self.config_file}.lock")

        with lock:

            config= self.read_config()
            config['resources'][self.title] = {
                'type': 'collection',
                'title': self.title,
                'description': f'creaf_forecast of precipitation',
                'keywords': ['country'],
                'extents': {
                    'spatial': {
                        'bbox': [min_x, min_y, max_x, max_y],
                        'crs': 'http://www.opengis.net/def/crs/EPSG/0/25830'
                    },
                },
                'providers': [
                    {
                        'type': 'edr',
                        'name': 'xarray-edr',
                        'data': self.zarr_out,
                        'x_field': 'longitude',
                        'y_field': 'latitude',
                        'time_field': 'time',
                        'format': {'name': 'zarr', 'mimetype': 'application/zip'},
                        #FIXME: check s3->anon: True should be working; maybe s3 vs http api
                        'options': {
                            's3': {
                                    'anon': False,
                                    'alternate_root': self.alternate_root,
                                    'endpoint_url': self.otc_endpoint,
                                    'requester_pays': False
                                }
                        }
                    }
                ]
            }

            self.write_config(config)


    def check_config_if_ds_is_collection(self):
        config = self.read_config()
        return self.title in config['resources']

    def execute(self, data):
        mimetype = 'application/json'
        #
        self.data_source = data.get('data_source')
        self.zarr_out = data.get('zarr_out')
        self.token = data.get('token')
        self.alternate_root = self.zarr_out.split("s3://")[1]

        if self.data_source is None:
            raise ProcessorExecuteError('Cannot process without a data path')
        if self.zarr_out is None or not self.zarr_out.startswith('s3://') :
            raise ProcessorExecuteError('Cannot process without a zarr path')
        if self.token is None:
            raise ProcessorExecuteError('Identify yourself with valid token!')

        if self.token != os.getenv("INT_API_TOKEN", "token"):
            #TODO Is this the correct error to return? Does this result in a BadRequest error?
            LOGGER.info("wrong internal API token received")
            raise ProcessorExecuteError('ACCESS DENIED wrong token')

        if self.zarr_out and self.zarr_out.startswith('s3://'):
            s3 = s3fs.S3FileSystem()
            if s3.exists(self.zarr_out):
                if self.title in self.read_config()['resources']:
                    msg = f"Path {self.zarr_out} already exists in bucket and config"
                else:
                    self.update_config()
                    msg = f"Path {self.zarr_out} already exists updates config at '{self.config_file}'"

                LOGGER.info(msg)
                return mimetype, {'id': 'creaf_forecast_ingestor', 'value': msg}


            else:
                store = s3fs.S3Map(root=self.zarr_out, s3=s3, check=False)
                data_path = download_source(self.data_source)
                tiff_da = tifs_to_ds(data_path)
                tiff_da.to_zarr(store=store, consolidated=True, mode='w')

                self.update_config()

                cleanup_data_temp()

                outputs = {
                    'id': 'creaf_forecast_ingestor',
                    'value': self.zarr_out
                }

                return mimetype, outputs

    def __repr__(self):
        return f'<IngestorCDSProcessProcessor> {self.name}'
