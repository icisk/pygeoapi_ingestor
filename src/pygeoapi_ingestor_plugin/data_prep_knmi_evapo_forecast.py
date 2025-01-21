from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
import s3fs

import os
import xarray as xr
import datetime
import numpy as np
import pandas as pd
from .utils import download_ftp_data, get_latest_forecast, get_data_from_cloud
import logging
from dotenv import load_dotenv, find_dotenv



LOGGER = logging.getLogger(__name__)

load_dotenv(find_dotenv())

PROCESS_METADATA = {
    'version': '0.2.0',
    'id': 'knmi_evapo_smhi_dataprep',
    'title': {
        'en': 'knmi_evapo_smhi_dataprep',
    },
    'description': {
        'en': 'downloads temp and precip data calculates potential evapotranspiration deltas per day and updates the online ressource'},
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
        },
        'status': {
            'title': 'status',
            'description': 'shows if new data got downloaded',
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


class RIJNLAND_evapo_dataprep_SMHI(BaseProcessor):
    """
    preparation process


    Resource Requirements:
    - CPU: 3
    - RAM: 12G
    """

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition

        """

        super().__init__(processor_def, PROCESS_METADATA)
        self.config_file = os.environ.get(default='/pygeoapi/serv-config/local.config.yml', key='PYGEOAPI_SERV_CONFIG')
        self.title = 'knmi_evapo'
        self.otc_key = os.environ.get(key='FSSPEC_S3_KEY')
        self.otc_secret = os.environ.get(key='FSSPEC_S3_SECRET')
        self.otc_endpoint = os.environ.get(key='FSSPEC_S3_ENDPOINT_URL')
        self.smhi_server = os.environ.get(key='SMHI_FTP_SERVER', default='ftp.smhi.se')
        self.smhi_user = os.environ.get(key='SMHI_FTP_USER', default='icisk')
        self.smhi_passwd = os.environ.get(key='SMHI_FTP_PASSWORD')
        self.smhi_fc_root = 'living_labs/netherlands/gridded_seasonal_forecast'
        self.alternate_root = None

        self.fc_zarr_out = None # "s3://52n-i-cisk/data-ingestor/smhi/rijnland/latest_fc.zarr"
        self.variable = None
        self.current_year = datetime.datetime.now().year
        self.current_month = datetime.datetime.now().month

        self.fc_save_path = '/tmp/knmi/fc'
        os.makedirs(self.fc_save_path, exist_ok=True)

        self.online_fc_data = None

    def get_Re(self, date, csv='/pygeoapi/tab_Re.csv'):
        y, m, d = date
        data = pd.read_csv(csv)
        return float(data['value'][data['x'] == f'{d}-{m}'].values[0])

    def calc_delta_PET(Ta, p, date):
        Re = get_Re(date)
        kc = 1
        lam = 2.501 - (0.002361 * Ta)
        rho = 1000
        Tadd = 5
        Tscale = 45
        PET_value = kc * (Re / (lam * rho)) * ((Ta + Tadd) / Tscale) * 1000
        PET = np.where((Ta + Tadd > 0), PET_value, 0)

        return PET - p

    def get_date(time):
        day = datetime.datetime.fromtimestamp(time.astype('datetime64[s]').astype(int)).day
        month = datetime.datetime.fromtimestamp(time.astype('datetime64[s]').astype(int)).month
        year = datetime.datetime.fromtimestamp(time.astype('datetime64[s]').astype(int)).year

        return (year, month, day)

    def download_and_process(self, latest_fc_path):
        download_ftp_data(self.smhi_server,
                          self.smhi_user,
                          self.smhi_passwd,
                          latest_fc_path,
                          self.fc_save_path)
        # read all forecast files and merge them then merge the 2 variables
        fc_data_path = os.path.join(self.fc_save_path, latest_fc_path)
        tas_files = [os.path.join(fc_data_path, p) for p in sorted(os.listdir(fc_data_path)) if
                     p.startswith('tas') and p.endswith('.nc')]
        pr_files = [os.path.join(fc_data_path, p) for p in sorted(os.listdir(fc_data_path)) if
                    p.startswith('pr') and p.endswith('.nc')]
        tas_ds = [xr.open_dataset(f).expand_dims(epoches=[i]) for i, f in enumerate(tas_files)]
        tas = xr.merge(tas_ds)
        pr_ds = [xr.open_dataset(f).expand_dims(epoches=[i]) for i, f in enumerate(pr_files)]
        pr = xr.merge(pr_ds)
        forecast_xr = xr.merge([tas, pr])
        vars = list(forecast_xr.data_vars)
        t_vals = forecast_xr[vars[0]].values
        p_vals = forecast_xr[vars[1]].values

        res = [calc_delta_PET(forecast_xr.isel(time=i).tasAdjust,
                              forecast_xr.isel(time=i).prAdjust,
                              get_date(t)) for i, t in enumerate(forecast_xr['time'].values)]

        res_arr = [ds.expand_dims(time=[forecast_xr['time'].values[i]]) for i, ds in enumerate(res)]
        res_res = xr.merge(res_arr)
        res_res = res_res.rename({'prAdjust': 'delta_precip_def'})
        percentiles = res_res.quantile([0.2, 0.5, 0.8], dim=["epoches"])

        return(percentiles)

    def execute(self, data):
        mimetype = 'application/json'


        self.fc_zarr_out = data.get('zarr_out')
        self.token = data.get('token')
        self.alternate_root = self.fc_zarr_out.split("s3://")[1] if self.fc_zarr_out is not None else "bibi"

        LOGGER.debug(f"checking process inputs")

        if self.fc_zarr_out and self.fc_zarr_out.startswith("s3://"):
            s3 = s3fs.S3FileSystem()
            if s3.exists(self.fc_zarr_out):
                self.online_fc_data = get_data_from_cloud(self.fc_zarr_out,
                                                          self.alternate_root,
                                                          self.otc_endpoint,
                                                          self.otc_key,
                                                          self.otc_secret)
            else:
                self.online_fc_data = None

        latest_fc_date, latest_fc_path = get_latest_forecast(self.smhi_server,
                                                             self.smhi_user,
                                                             self.smhi_passwd,
                                                             self.smhi_fc_root)
        if self.online_fc_data:
            LOGGER.debug(f'forecast in otc')
            online_fc_date = datetime.datetime.fromtimestamp(self.online_fc_data['time'].data[0].astype('datetime64[s]').astype(int))
            if online_fc_date < latest_fc_date:
                LOGGER.debug(f'new fc available')
                download_fc = True
            else:
                LOGGER.debug(f'no new fc available')
                download_fc = False
        else:
            LOGGER.debug(f'no forecast in otc')
            download_fc = True

        if download_fc:
            LOGGER.debug(f'download from ftp')
            forecast_xr = self.download_and_process(latest_fc_path)


            LOGGER.debug(f'upload to otc')
            s3 = s3fs.S3FileSystem()
            store = s3fs.S3Map(root=self.fc_zarr_out, s3=s3, check=False)
            forecast_xr.to_zarr(store=store, consolidated=True, mode='w')

            status_msg=f'downloaded new forecast data'
        else:
            status_msg=f'no new forecast available'

        outputs = {
            'id': 'll7_smhi_forecast',
            'value': self.fc_zarr_out,
            'status': status_msg
        }

        return mimetype, outputs

    def __repr__(self):
        return f'<RIJNLAND_evapo_dataprep_SMHI> {self.name}'
