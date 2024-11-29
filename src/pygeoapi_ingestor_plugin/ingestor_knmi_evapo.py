from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from filelock import Timeout, FileLock

import fsspec
import s3fs
import yaml

import os
import subprocess
import io
import requests
import xarray as xr
import datetime
import pandas as pd
import numpy as np
import rioxarray
import rasterio
from rasterio.io import MemoryFile


from osgeo import gdal

import logging
from dotenv import load_dotenv, find_dotenv


from .utils import download_source

LOGGER = logging.getLogger(__name__)

load_dotenv(find_dotenv())

PROCESS_METADATA = {
    'version': '0.2.0',
    'id': 'knmi_evapo_ingestor',
    'title': {
        'en': 'knmi_evapo',
    },
    'description': {
        'en': 'downloads temp and precip data calculates potential evapotranspiration and cumsums it updating the online ressource'},
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
        'zarr_base': {
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
            "zarr_out": "s3://example/target/bucket.zarr",
            "token": "ABC123XYZ666"
        }
    }
}


class IngestorKNMIProcessProcessor(BaseProcessor):
    """
    Ingestor Processor

    joins tiff-data and saves it to zarr format and uploads it to s3 bucket

    Resource Requirements:
    - CPU: 3
    - RAM: 12G
    """

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition

        :returns: pygeoapi.process.ingestor_process.IngestorCREAFHISTORICProcessProcessor
        """

        super().__init__(processor_def, PROCESS_METADATA)
        self.config_file = os.environ.get(default='/pygeoapi/serv-config/local.config.yml', key='PYGEOAPI_SERV_CONFIG')
        self.title = 'knmi_evapo'
        self.otc_key = os.environ.get(key='FSSPEC_S3_KEY')
        self.otc_secret = os.environ.get(key='FSSPEC_S3_SECRET')
        self.otc_endpoint = os.environ.get(key='FSSPEC_S3_ENDPOINT_URL')
        self.knmi_api_key = os.environ.get(key='KNMI_API_KEY')
        self.knmi_header = dict(Authorization=self.knmi_api_key)
        self.alternate_root = None
        self.data_path = None
        self.zarr_base = None
        self.zarr_out = None
        self.variable = None
        self.current_year = datetime.datetime.now().year
        self.mode = 'init' # 'init' - first time  or 'append' - every other time
        self.save_path = '/tmp/knmi'
        os.makedirs(self.save_path, exist_ok=True)
        self.online_data = None

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
        min_x = float(da.x.values.min())
        max_x = float(da.x.values.max())
        min_y = float(da.y.values.min())
        max_y = float(da.y.values.max())

        # THIS MUST BE THE SAME IN ALL PROCESSES UPDATING THE SERV CONFIG
        lock = FileLock(f"{self.config_file}.lock")

        with lock:

            config= self.read_config()
            config['resources'][f'{self.title}_{self.current_year}'] = {
                'type': 'collection',
                'title': f'{self.title}_{self.current_year}',
                'description': f'{self.title} of {self.current_year}',
                'keywords': ['country'],
                'extents': {
                    'spatial': {
                        'bbox': [min_x, min_y, max_x, max_y],
                        'crs': 'http://www.opengis.net/def/crs/EPSG/0/28992'
                    },
                },
                'providers': [
                    {
                        'type': 'edr',
                        'name': 'xarray-edr',
                        'data': self.zarr_out,
                        'x_field': 'x',
                        'y_field': 'y',
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

    def format_t(self, y, m, d, t=1, format='%Y%m%d'):
        d_start = datetime.date(y, m, d)
        td = datetime.timedelta(days=t)
        d_end = d_start + td
        return (d_start.strftime(format), d_end.strftime(format))

    def download_date_data(self,
                           date,
                           header,
                           params=dict(maxKeys=10, sorting="desc"),
                           data_source=dict(precip={
                               "base_url": 'https://api.dataplatform.knmi.nl/open-data/v1/datasets/Rd1nrt/versions/2/files'
                           }, temp={
                               "base_url": 'https://api.dataplatform.knmi.nl/open-data/v1/datasets/Tg1/versions/5/files'
                           })):
        save_path = self.save_path
        y, m, d = date
        ds, de = self.format_t(y, m, d)
        data_source['temp']['filename'] = f'INTER_OPER_R___TG1_____L3__{ds}T000000_{de}T000000_0006.nc'
        data_source['precip']['filename'] = f'INTER_OPER_R___RD1NRT__L3__{ds}T080000_{de}T080000_0002.nc'

        # #getting latest ds name
        # try:
        #     res_filename = requests.get(var_url, params=params, headers=header)
        #     res_filename.raise_for_status()
        #     data_filename = res_filename.json()
        #     latest_file = data_filename['files'][0]['filename']
        #     #TODO: check for latest file in zarr archive before //outside func
        #     #TODO: check if files exists already //outside func
        # except requests.exceptions.HTTPError as e:
        #     print(f""" latest file error '{str(e)}'; HTML-code '{res_filename.status_code}'""")
        for var in data_source:
            print(var)
            source = data_source[var]
            # getting date ds url
            try:
                res_dwn_url = requests.get(os.path.join(source['base_url'], source['filename'], 'url'), headers=header)
                res_dwn_url.raise_for_status()
                data_dwn_url = res_dwn_url.json()
                dwn_url = data_dwn_url['temporaryDownloadUrl']
            except requests.exceptions.HTTPError as e:
                print(f""" download url error '{str(e)}'; HTML-code '{res_dwn_url.status_code}'""")
                return None

            # getting data itself
            try:
                res_data = requests.get(dwn_url)
                res_data.raise_for_status()
                data = res_data.content
                with open(os.path.join(save_path, source['filename']), 'wb') as f:
                    f.write(data)
            except requests.exceptions.HTTPError as e:
                f""" download data error '{str(e)}'; HTML-code '{res_data.status_code}'"""
                return None
            except IOError as e:
                print(f"error writing file: {str(e)}")
                return None

            data_source[var]['output_path'] = os.path.join(save_path, source['filename'])

        return (data_source)

    def get_Re(self, date, csv='/pygeoapi/tab_Re.csv'):
        y, m, d = date
        data = pd.read_csv(csv)
        return float(data['value'][data['x'] == f'{d}-{m}'].values[0])

    def calc_PET(self, Ta, date):
        Re = self.get_Re(date)
        kc = 1
        lam = 2.501 - (0.002361 * Ta)
        rho = 1000
        Tadd = 5
        Tscale = 45

        PET = np.where(Ta + Tadd > 0,
                       kc * (Re / (lam * rho)) * ((Ta + Tadd) / Tscale) * 1000,
                       0)

        return PET


    def execute(self, data):
        mimetype = 'application/json'
        #FIXME: hier token aus data lesen --> invoke nicht vergessen wa
        #process_token = data.get("precess_token")
        #

        self.zarr_out = data.get('zarr_out')
        self.token = data.get('token')
        self.alternate_root = self.zarr_out.split("s3://")[1] if self.zarr_out is not None else "bibi"

        LOGGER.debug(f"checking process inputs")

        if self.zarr_out is None or not self.zarr_out.startswith('s3://') :
            raise ProcessorExecuteError('Cannot process without a zarr path')
        if self.token is None:
            raise ProcessorExecuteError('Identify yourself with valid token!')

        if self.token != os.getenv("INT_API_TOKEN", "token"):
            #FIXME passender error?
            LOGGER.error("WRONG INTERNAL API TOKEN")
            raise ProcessorExecuteError('ACCESS DENIED wrong token')

        LOGGER.debug(f"checking online resources")
        if self.zarr_out and self.zarr_out.startswith('s3://'):
            s3 = s3fs.S3FileSystem()
            if s3.exists(self.zarr_out):
                self.online_data = self.get_data_from_cloud()
                if self.title in self.read_config()['resources']:
                    self.mode = 'append'
                    msg = f"Path {self.zarr_out} already exists in bucket and config; appending to dataset"
                else:
                    #FIXME gescheiten exit finden
                    self.update_config()
                    self.mode = 'append'
                    msg = f"Path {self.zarr_out} already exists updates config at '{self.config_file}'; appending to dataset"
                #return mimetype, {'id': 'creaf_historic_ingestor', 'value': msg}
            else:
                msg = f"No online ressource '{self.zarr_out}' available; initially creating one"

            LOGGER.info(msg)


        store = s3fs.S3Map(root=self.zarr_out, s3=s3, check=False)
        if self.mode == 'init':
            date = datetime.date(self.current_year, 4, 1)

        if self.mode == 'append':
            date = datetime.datetime.fromtimestamp(self.online_data.time.values[-1].astype('datetime64[s]').astype(int))
            date = date + datetime.timedelta(days=1)


        y = date.year
        m = date.month
        d = date.day

        LOGGER.debug(f"download data from date: '{y}-{m}-{d}'")
        summary = self.download_date_data((y, m, d), header=self.knmi_header)
        if summary is None:
            return mimetype, {'id': 'knmi_ingestor', 'value': 'process_failed!'}

        LOGGER.debug(f"processing files")
        t = xr.open_dataset(summary['temp']['output_path'])
        p = xr.open_dataset(summary['precip']['output_path'])

        t_vals = t['prediction'].values
        pet_vals = self.calc_PET(t_vals, (2024, 7, 15))
        p_vals = p['prediction'].values
        p_def = pet_vals - p_vals
        res = xr.Dataset(
            data_vars=dict(p_def=(['time', 'y', 'x'], p_def)),
            coords=dict(
                time=t.time.data,
                y=t.y.data,
                x=t.x.data
            )
        )

        LOGGER.debug(f"upload ZARR: '{self.zarr_out}' in '{self.mode}-mode'")
        date_string = datetime.date(y, m , d).strftime('%Y%m%d')
        export_tif_path =f"s3://52n-i-cisk/tif/LL_Rijnland/evapo/evapo_{date_string}.tif"

        if self.mode == 'init':
            res.to_zarr(store=store, consolidated=True, mode='w')
            res.to_netcdf(f"/tmp/evapo_{date_string}.nc")
        if self.mode == 'append':
            new_p_def = self.online_data.isel(time=-1)['p_def'] + res.isel(time=0)['p_def']
            new_res = xr.Dataset(
                data_vars=dict(p_def=(['time', 'y', 'x'], np.expand_dims(new_p_def.data, axis=0))),
                coords=dict(
                    time=t.time.data,
                    y=t.y.data,
                    x=t.x.data
                )
            )
            new_res.to_zarr(store=store, consolidated=True, append_dim='time', mode='a')
            new_res.to_netcdf(f"/tmp/evapo_{date_string}.nc")


        command = f"gdal_translate -a_ullr {float(res.x.values.min())} {float(res.y.values.max())} {float(res.x.values.max())} {float(res.y.values.min())} -a_srs EPSG:28992 /tmp/evapo_{date_string}.nc /tmp/evapo_{date_string}.tif"
        LOGGER.debug(command)
        try:
            subprocess.run(command, shell=True, check=True)
            print("gdal_transform completed successfully.")
        except subprocess.CalledProcessError as e:
            print(f"Error: {e}")

        with open(f'/tmp/evapo_{date_string}.tif', 'rb') as file:
            tiff_data = file.read()
        with s3.open(export_tif_path, "wb") as tif:
            tif.write(tiff_data)

        self.update_config()

        outputs = {
            'id': 'knmi_ingestor',
            'value': self.zarr_out
        }

        return mimetype, outputs

    def __repr__(self):
        return f'<IngestorKNMIProcessProcessor> {self.name}'
