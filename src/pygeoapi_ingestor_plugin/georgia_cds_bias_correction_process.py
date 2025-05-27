import os
import json
import datetime
from dateutil.relativedelta import relativedelta
import logging
import tempfile
from filelock import FileLock

import numpy as np
import pandas as pd
import xarray as xr
from scipy import stats
from scipy.interpolate import splev

import s3fs
import cdsapi

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

import pygeoapi_ingestor_plugin.utils as utils
import pygeoapi_ingestor_plugin.utils_s3 as s3_utils

from ..invoke.invoke_ingestor import invoke_ingestor_process

# =================================================================
#
# Authors: Tommaso Redaelli <tommaso.redaelli@gecosistema.com>, Micha Werner <m.werner@un-ihe.org>
#
# Copyright (c) 2023 Tommaso Redaelli, Micha Werner
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
    "version": "0.2.0",
    "id": "safer-process",
    "title": {
        "en": "Georgia Bias CDS Correction",
    },
    "description": {
        "en": "Georgia Bias CDS Correction CDS procedure for CDS Seasonal Forecast data in Georgia",
    },
    "jobControlOptions": [
        "sync-execute",
        "async-execute"
    ],
    "keywords": [
        "safer process"
    ],
    "inputs": {
        "token": {
            "title": "secret token",
            "description": "identify yourself",
            "schema": {
                "type": "string"
            }
        },
        "debug": {
            "title": "Debug",
            "description": "Enable Debug mode",
            "schema": {}
        },
    },
    "outputs": {
        "status": {
            "title": "status",
            "description": "Staus of the process execution [OK or KO]",
            "schema": {}
        },
        "data": {
            "title": "data",
            "description": "The data output file"
        },
    },
    "example": {
        "inputs": {
            "debug": True,
            "token": "ABC123XYZ666",
            "start_month": "2025-01"
        }
    },
}


class Handle200Exception(Exception):
    OK = "OK"
    SKIPPED = "SKIPPED"
    DENIED = "DENIED"

    def __init__(self, status, message):
        self.status = status
        self.message = message
        super().__init__(self.message)


class GeorgiaCDSBiasCorrectionProcessProcessor(BaseProcessor):
    """
    Georgia CDS Bias Correction Processor
    """

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

        self.cds_client = cdsapi.Client(url="https://cds.climate.copernicus.eu/api", key=os.getenv("CDSAPI_KEY"))

        self.living_lab = "georgia"  # DOC: This processor is specific for Georgia living lab
        
        self.config_file = os.environ.get(key='PYGEOAPI_SERV_CONFIG', default='/pygeoapi/serv-config/local.config.yml')

        self.process_temp_dir = os.path.join(tempfile.gettempdir(), "georgia_cds_bias_correction_process")
        if not os.path.exists(self.process_temp_dir):
            os.makedirs(self.process_temp_dir, exist_ok=True)

        self.process_data_dir = os.path.join(os.getcwd(), "georgia_cds_bias_correction_process")
    
    
    
    def run_cds_ingestor_process(self): 
        tp_seasonal_forecast_invcation_data = 'src/invoke/invocation_data/cds-georgia-seasonal-original-single-levels-total_precipitation-ingest-input.json'
        t2m_seasonal_forecast_invcation_data = 'src/invoke/invocation_data/cds-georgia-seasonal-original-single-levels-2m_temperature-ingest-input.json'
        
        def invoke_ingestor(invocation_data):
            with open(invocation_data, "r") as f:
                data = json.load(f)

            ingestor_process = data["process"]
            payload = data["payload"]

            api_root = os.getenv("API_ROOT", "http://localhost:5000/")
            execute_url = f"{api_root}processes/{ingestor_process}/execution"

            token = os.getenv("INT_API_TOKEN", "token")
            payload["inputs"]["token"] = token
            
            # !!!: This is pretty ugly, zarr-output and collection-name should be returned by process execution output and then returned by invoke_ingestor. If names logic in ingestor_cds_process.py changes, this will break.
            current_datetime = datetime.datetime.now(datetime.timezone.utc)
            cds_variable = payload["inputs"]["query"]["variable"][0]
            collection_variable = {'total_precipitation': 'tp', '2m_temperature': 't2m'}[cds_variable]
            zarr_out = f"cds_{self.living_lab}-seasonal-original-single-levels_{cds_variable}_{current_datetime.strftime('%Y%m')}.zarr"
            collection_name = f"seasonal-original-single-levels_{current_datetime.strftime('%Y%m')}_{self.living_lab}_{collection_variable}_0"
            
            # Run ingestor_cds_process
            invoke_ingestor_process(execute_url=execute_url, data=payload, logger=LOGGER)
            
            return zarr_out, collection_name
            
        tp_zarr, tp_collection = invoke_ingestor(tp_seasonal_forecast_invcation_data)
        t2m_zarr, t2m_collection = invoke_ingestor(t2m_seasonal_forecast_invcation_data)
        
        return (tp_zarr, tp_collection), (t2m_zarr, t2m_collection)

    
    def collection_data_uri(self, zarr_filename):
        """
        Generate the S3 URI for the collection dataset.
        """
        return f"s3://{os.environ['DEFAULT_BUCKET']}/{os.environ['DEFAULT_REMOTE_DIR']}/{self.living_lab}/seasonal_timeseries/{zarr_filename}"
    
    
    def get_collection_dataset(self, zarr_filename):
        ds = xr.open_zarr(
            store=self.collection_data_uri(zarr_filename),
            consolidated=True,
            chunks="auto",
            zarr_version=2
        )
        return ds
        
        
    def tp_bias_correction(self, tp_zarr):
        """
        Apply bias correction to the total precipitation data.
        This is a placeholder for the actual bias correction logic.
        """
        
        ds = self.get_collection_dataset(tp_zarr)
        ds.sel(time = ds.time.dt.month <= (ds.forecast_reference_time[0].dt.date + relativedelta(months=6)).item().month)   # DOC: we keep only forecast inside 6 months from the reference time month
        
        biascorrect_model_observed = pd.read_csv(f'../data/{self.living_lab}/bias_correction/bias_correction_observed_gamma.csv')
        biascorrect_model_forecast = pd.read_csv(f'../data/{self.living_lab}/bias_correction/bias_correction_forecast_gamma.csv')
        
        ds_vars = [v for v in list(ds.variables.keys()) if v not in list(ds.coords)]
        
        # Apply bias correction
        for point_lat in sorted(set(ds.latitude.values)):
    
            for point_lon in sorted(set(ds.longitude.values)):
                
                bc_obs = biascorrect_model_observed[
                    (biascorrect_model_observed['initmonth']==ds.forecast_reference_time[0].dt.month.item()) &
                    (biascorrect_model_observed['lat']==point_lat) &
                    (biascorrect_model_observed['lon']==point_lon) & 
                    (biascorrect_model_observed['variable']=='tp')
                ]
                fc_obs = biascorrect_model_forecast[
                    (biascorrect_model_forecast['initmonth']==ds.forecast_reference_time[0].dt.month.item()) &
                    (biascorrect_model_forecast['lat']==point_lat) &
                    (biascorrect_model_forecast['lon']==point_lon) & 
                    (biascorrect_model_forecast['variable']=='tp')
                ]

                for ilead in sorted(set(ds.time.dt.month.values)):
                    
                    ilead = ilead - ds.forecast_reference_time.dt.month.item() 
                    
                    if ilead >= 7:
                        continue
                    
                    gamma_obs_pars = bc_obs[bc_obs['leadtime']==ilead].iloc[0]
                    gammma_fc_pars = fc_obs[fc_obs['leadtime']==ilead].iloc[0]
                    
                    # transform fcst values to the cdf of the gamma distribution
                    rv = stats.gamma(gammma_fc_pars.alpha, loc=0., scale=gammma_fc_pars.scale)
                    rvo = stats.gamma(gamma_obs_pars.alpha, loc=0., scale=gamma_obs_pars.scale)
                    
                    for var in ds_vars:
                        
                        fc_month_values = ds.sel(
                            forecast_reference_time = ds.forecast_reference_time[0],
                            latitude = point_lat,
                            longitude = point_lon,
                            time = ds.time.dt.month == ilead
                        )[var].values

                        cdf =  rv.cdf(fc_month_values)
                        
                        # transform the values to the normal space
                        monthValuesN = stats.norm.ppf(cdf)
                        
                        # now go back using the Gamma distributuin of the obs        
                        monthValuesB = rvo.ppf(stats.norm.cdf(monthValuesN))
                        
                        ds[var].loc[dict(
                            forecast_reference_time = ds.forecast_reference_time[0],
                            latitude = point_lat,
                            longitude = point_lon,
                            time = ds.time.dt.month == ilead
                        )] = monthValuesB.astype(np.float32)
                        
        return ds
    
    
    def upload_s3_bc_zarr(self, og_zarr, bc_ds):
        bc_zarr = og_zarr.replace("'.zarr", "__bias-corrected.zarr")
        s3 = s3fs.S3FileSystem()
        s3_store = s3fs.S3Map(root=bc_zarr, s3=s3, check=False)
        bc_ds.to_zarr(store=s3_store, consolidated=True, mode='w')
        return bc_zarr
            
        
    def update_pygeoapi_config(self, bc_zarr, bc_ds, collection_name):
        """
        Update the pygeoapi configuration with the new dataset.
        This is a placeholder for the actual configuration update logic.
        """
        lock = FileLock(f"{self.config_file}.lock", thread_local=False)
        with lock:
            config = utils.read_config(self.config_file)
            collection = config['resources'][collection_name]
            
            # DOC: ensure collection metadata is updated
            min_x, max_x = bc_ds.longitude.min().item(), bc_ds.longitude.max().item()
            min_y, max_y = bc_ds.latitude.min().item(), bc_ds.latitude.max().item()
            min_dt, max_dt = datetime.datetime.fromisoformat(bc_ds.time.min().dt.date.item().isoformat()), datetime.datetime.fromisoformat(bc_ds.time.max().dt.date.item().isoformat())
            collection['extents'] = {
                'spatial': { 'bbox': [min_x, min_y, max_x, max_y], 'crs': 'http://www.opengis.net/def/crs/OGC/1.3/CRS84' },
                'temporal': { 'begin': min_dt, 'end': max_dt }
            }
            collection['providers'][0]['data'] = bc_zarr
            s3_is_anon_access = s3_utils.is_s3_anon_access()
            if not s3_is_anon_access:
                alternate_root = self.collection_uri.split("s3://")[1]
                endpoint_url = os.environ.get(default="https://obs.eu-de.otc.t-systems.com", key='FSSPEC_S3_ENDPOINT_URL')
                collection['providers'][0]['options'] = {
                    's3': { 'anon': False, 'alternate_root': alternate_root, 'endpoint_url': endpoint_url, 'requester_pays': False }
                }
                
            config['resources'][collection_name] = collection
            
        utils.write_config(config_path=self.config_file, config_out=config)
        LOGGER.info(f"collection definition modified: '{config['resources'][collection_name]}'")
        
        
    def apply_bias_correction(self, tp_ingest_info, t2m_ingest_info):
        
        # DOC: bias correcting total precipitation (tp) data
        tp_zarr, tp_collection = tp_ingest_info
        tp_bc_ds = self.tp_bias_correction(tp_zarr)
        tp_bc_zarr = self.upload_s3_bc_zarr(tp_zarr, tp_bc_ds)
        self.update_pygeoapi_config(tp_bc_zarr, tp_bc_ds, tp_collection)
        
        # DOC: bias correcting 2m temperature (t2m) data
        t2m_zarr, t2m_collection = t2m_ingest_info
        # self.t2m_bias_correction(t2m_zarr, t2m_collection)
        
        return (tp_bc_zarr, tp_collection), (None, None)  # TODO: Implement T2M bias correction
        
        
        
    def execute(self, data):
        mimetype = "application/json"

        outputs = {}
        try:
            tp_ingest_info, t2m_ingest_info = self.run_cds_ingestor_process()
            
            tp_bc_info, t2m_bc_info = self.apply_bias_correction(tp_ingest_info, t2m_ingest_info)
            
            outputs = {
                "status": "OK",
                
                "total_precipitation": {
                    "zarr": tp_bc_info[0],
                    "collection": tp_bc_info[1]
                },
                
                "2m_temperature": {
                    "zarr": t2m_bc_info[0],
                    "collection": t2m_bc_info[1]
                }
            }

        except Handle200Exception as err:
            outputs = {"status": err.status, "message": str(err)}
        except Exception as err:
            raise ProcessorExecuteError(str(err)) from err

        return mimetype, outputs
    

    def __repr__(self):
        return f"<GeorgiaCDSBiasCorrectionProcessProcessor> {self.name}"