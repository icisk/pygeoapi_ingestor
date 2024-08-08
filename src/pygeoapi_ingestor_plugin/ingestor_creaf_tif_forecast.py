from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

import os
import s3fs
import yaml

import tifffile as tiff
import numpy as np
import xarray as xr

from osgeo import gdal

import logging
from dotenv import load_dotenv, find_dotenv

LOGGER = logging.getLogger(__name__)

load_dotenv(find_dotenv())

PROCESS_METADATA = {
    'version': '0.2.0',
    'id': 'creaf_forecast_ingestor',
    'title': {
        'en': 'Ingestor Process',
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
        'data_path': {
            'title': 'data_path',
            'description': 'path to the data locally',
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
            "data_path": "/data/creaf/precip_forecast",
            "zarr_out": "irgendwas"
        }
    }
}


def get_pixel_centroids(file_path):
    ds = gdal.Open(file_path)
    gt = ds.GetGeoTransform()
    x_size, y_size = ds.RasterXSize, ds.RasterYSize

    x_centroids = [gt[0] + gt[1] * (x + 0.5) for x in range(x_size)]
    y_centroids = [gt[3] + gt[2] * (y + 0.5) for y in range(y_size)]

    return x_centroids, y_centroids


def tifs_to_da(path):
    #file paths
    files = [os.path.join(path, f) for f in sorted(os.listdir(path)) if f.endswith('.tif') or f.endswith('.tiff')]
    #naming and metadata
    file_names = [os.path.splitext(f)[0] for f in sorted(os.listdir(path))]
    info = [f"{int(parts[0]):02}_{parts[1]}_{parts[2]}" for fn in file_names for parts in [fn.split("_")[1:4]]]
    time = [np.datetime64(f'{parts[1]}-{parts[0]}-01T00:{i:02}') for i, date in enumerate(info) for parts in [date.split("_")]]
    x, y = get_pixel_centroids(files[0])
    #xarray creation
    arrays = [tiff.imread(file) for file in files]
    stacked = np.stack(arrays, axis=0)
    da = xr.DataArray(stacked, 
                      dims=['time', 'latitude', 'longitude'], 
                      coords={'time': time, 'latitude': y, 'longitude': x}, 
                      attrs={'info': info,
                             'long_name': 'geh mir nich aufn sack',
                             'units': 'deine mudder'}, 
                      name='data')

    return da

def update_config(da, zarr_out):
    #FIXME hier zarr aus der cloud holen/ lesen um metadaten zu basteln
    min_x = float(da.longitude.values.min())
    max_x = float(da.longitude.values.max())
    min_y = float(da.latitude.values.min())
    max_y = float(da.latitude.values.max())
    endpoint_url = os.environ.get(default="https://obs.eu-de.otc.t-systems.com", key='FSSPEC_S3_ENDPOINT_URL')
    alternate_root = zarr_out.split("s3://")[1]

    #FIXME use env PYGEOAPI_CONFIG
    with open('/pygeoapi/local.config.yml','r' ) as file:
            config = yaml.safe_load(file)

    config['resources'][f"creaf_forecast"] = {
        'type': 'collection',
        'title': f"creaf_forecast",
        'description': f'creaf_forecast',
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
                'data': zarr_out,
                'x_field': 'longitude',
                'y_field': 'latitude',
                'time_field': 'time',
                'format': {'name': 'zarr', 'mimetype': 'application/zip'},
                #FIXME: anon checken: sollte true sein; s3 vs http api 
                'options': {
                    's3': {
                            'anon': False, 
                            'requester_pays': False, 
                            "alternate_root": alternate_root, 
                            "endpoint_url": endpoint_url
                        }
                }
            }
        ]
    }

    #FIXME use env PYGEOAPI_CONFIG
    with  open('/pygeoapi/local.config.yml', 'w') as outfile:
        yaml.dump(config, outfile, default_flow_style=False)
    LOGGER.debug("updated config")


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

    def execute(self, data):
        mimetype = 'application/json'
        #FIXME: hier token aus data lesen --> invoke nicht vergessen wa
        #process_token = data.get("precess_token")
        #
        creaf_forecast_tif_path = data.get('data_path')
        zarr_out = data.get('zarr_out')

        if creaf_forecast_tif_path is None:
            raise ProcessorExecuteError('Cannot process without a data path')
        if zarr_out is None or not zarr_out.startswith('s3://') :
            raise ProcessorExecuteError('Cannot process without a zarr path')

        if zarr_out and zarr_out.startswith('s3://'):
            s3 = s3fs.S3FileSystem()
            if s3.exists(zarr_out):
                #FIXME collection in config neu erstellen
                raise ProcessorExecuteError(f'Path {zarr_out} already exists')

        store = s3fs.S3Map(root=zarr_out, s3=s3, check=False)
        tiff_da = tifs_to_da(creaf_forecast_tif_path)
        tiff_da.to_zarr(store=store, consolidated=True, mode='w')

        update_config(tiff_da, zarr_out)

        outputs = {
            'id': 'creaf_forecast_ingestor',
            'value': zarr_out
        }

        return mimetype, outputs

    def __repr__(self):
        return f'<IngestorCDSProcessProcessor> {self.name}'
