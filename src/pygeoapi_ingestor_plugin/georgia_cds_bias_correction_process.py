import os
import sys
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

from pygeoapi_ingestor_plugin.ingestor_cds_process import IngestorCDSProcessProcessor, PROCESS_METADATA as IngestorCDSProcessProcessor_METADATA

import pygeoapi_ingestor_plugin.utils as utils
import pygeoapi_ingestor_plugin.utils_s3 as s3_utils

# =================================================================
#
# Authors: Tommaso Redaelli <tommaso.redaelli@gecosistema.com> 
#
# Copyright (c) 2025 Tommaso Redaelli, Micha Werner <m.werner@un-ihe.org>
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
    PARTIAL = "PARTIAL"
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
    
    
    # DOC: 1. Validate parameters
    def validate_parameters(self, data):
        token = data.get("token", None)
        cron_invocation = data.get("cron_invocation", False)

        if token is None:
            raise ProcessorExecuteError("You must provide an valid token")
        if token != os.getenv("INT_API_TOKEN", "token"):
            LOGGER.error(
                f"WRONG INTERNAL API TOKEN {token} ({type(token)}) != {os.getenv('INT_API_TOKEN', 'token')} ({type(os.getenv('INT_API_TOKEN', 'token'))})"
            )
            raise Handle200Exception(Handle200Exception.DENIED, "ACCESS DENIED: wrong token")
        
        if not isinstance(cron_invocation, bool):
            LOGGER.error(f"cron_invocation must be a boolean, got {type(cron_invocation)}")
            raise ProcessorExecuteError("cron_invocation must be a boolean")
        if not cron_invocation:
            LOGGER.error("This process is intended to be run as a cron job, not manually.")
            raise Handle200Exception(Handle200Exception.SKIPPED, "This process is intended to be run as a cron job, not manually.")
        
    
    # DOC: 2. Run CDS ingestor process
    def run_cds_ingestor_process(self): 
        IngestorCDSProcessProcessor_METADATA['name'] = IngestorCDSProcessProcessor_METADATA['id']
        ingestor_cds_process_processor =  IngestorCDSProcessProcessor(IngestorCDSProcessProcessor_METADATA)
        
        tp_seasonal_forecast_invcation_data = 'src/invoke/invocation_data/cds-georgia-seasonal-original-single-levels-total_precipitation-ingest-input.json'
        t2m_min_seasonal_forecast_invcation_data = 'src/invoke/invocation_data/cds-georgia-seasonal-original-single-levels-minimum_2m_temperature_in_the_last_24_hours-ingest-input.json'
        t2m_max_seasonal_forecast_invcation_data = 'src/invoke/invocation_data/cds-georgia-seasonal-original-single-levels-maximum_2m_temperature_in_the_last_24_hours-ingest-input.json'
        
        def invoke_ingestor(invocation_data):
            with open(invocation_data, "r") as f:
                data = json.load(f)

            payload = data["payload"]
            
            token = os.getenv("INT_API_TOKEN", "token")
            payload["inputs"]["token"] = token
            
            # !!!: Zarr-output and collection-name should be returned by process execution output and then returned by invoke_ingestor (by now it returns just zarr-out). If names logic in ingestor_cds_process.py changes, this will break.
            current_datetime = datetime.datetime.now(datetime.timezone.utc)
            cds_variable = payload["inputs"]["query"]["variable"][0]
            collection_variable = {
                'total_precipitation': 'tp',
                '2m_temperature': 't2m',
                'minimum_2m_temperature_in_the_last_24_hours': 'mn2t24',
                'maximum_2m_temperature_in_the_last_24_hours': 'mx2t24'
            }[cds_variable]
            
            zarr_out = f"cds_{self.living_lab}-seasonal-original-single-levels_{cds_variable}_{current_datetime.strftime('%Y%m')}.zarr"
            collection_name = f"seasonal-original-single-levels_{current_datetime.strftime('%Y%m')}_{self.living_lab}_{collection_variable}_0"
            
            ingestor_cds_process_processor.execute(data=payload['inputs'])
            
            return zarr_out, collection_name
        
        LOGGER.info(f"Invoking ingestor for total precipitation (tp) seasonal forecast data")            
        tp_zarr, tp_collection = invoke_ingestor(tp_seasonal_forecast_invcation_data)
        
        LOGGER.info(f"Invoking ingestor for minimum 2m temperature (t2m_min) seasonal forecast data")
        t2m_min_zarr, t2m_min_collection = invoke_ingestor(t2m_min_seasonal_forecast_invcation_data)
        
        LOGGER.info(f"Invoking ingestor for maximum 2m temperature (t2m_max) seasonal forecast data") 
        t2m_max_zarr, t2m_max_collection = invoke_ingestor(t2m_max_seasonal_forecast_invcation_data)
        
        return (tp_zarr, tp_collection), (t2m_min_zarr, t2m_min_collection), (t2m_max_zarr, t2m_max_collection) 


    def og_to_bc_zarr_filename(self, og_zarr_filename):
        return og_zarr_filename.replace(".zarr", "__bias-corrected.zarr")

    
    def collection_data_uri(self, zarr_filename):
        """
        Generate the S3 URI for the collection dataset.
        """
        return f"s3://{os.path.join(os.environ['DEFAULT_BUCKET'], os.environ['DEFAULT_REMOTE_DIR'], self.living_lab, 'seasonal_timeseries', zarr_filename)}"
    
    
    def get_collection_dataset(self, zarr_filename):
        LOGGER.info(f"Loading dataset from {self.collection_data_uri(zarr_filename)}")
        ds = xr.open_zarr(
            store=self.collection_data_uri(zarr_filename),
            consolidated=True,
            chunks="auto",
            zarr_version=2
        )
        ds_local_filepath = os.path.join(self.process_temp_dir, f"{zarr_filename}.zarr")
        ds.to_netcdf(ds_local_filepath)
        return ds_local_filepath
    
    
    # DOC: 3. Create bias-corrected collection for total precipitation (tp) and 2m temperature (t2m_min, t2m_max)
    def create_bias_corrected_collection(self, tp_ingest_info, t2m_min_ingest_info, t2m_max_ingest_info):
        
        # DOC: bias correcting total precipitation (tp) data
        
        LOGGER.info("\n### Start bias correction for total precipitation (tp) data\n")
        
        tp_zarr, tp_collection = tp_ingest_info
        tp_source = self.ensure_collection_source(tp_zarr, tp_collection)
        if tp_source is None:
            tp_bc_ds = self.bias_correction_procedure(tp_zarr, bc_var='tp')
            tp_bc_zarr_uri = self.upload_s3_bc_zarr(tp_zarr, tp_bc_ds)
            self.update_pygeoapi_config(tp_bc_zarr_uri, tp_bc_ds, tp_collection)
        else:
            tp_bc_zarr_uri, tp_collection = tp_source
        
        # DOC: bias correcting minimum 2m temperature data
        
        LOGGER.info("\n### Start bias correction for minimum 2m temperature (t2m_min) data\n")
        
        t2m_min_zarr, t2m_min_collection = t2m_min_ingest_info
        t2m_min_source = self.ensure_collection_source(t2m_min_zarr, t2m_min_collection)
        if t2m_min_source is None:
            t2m_min_bc_ds = self.bias_correction_procedure(t2m_min_zarr, bc_var='tmn')
            t2m_min_bc_zarr_uri = self.upload_s3_bc_zarr(t2m_min_zarr, t2m_min_bc_ds)
            self.update_pygeoapi_config(t2m_min_bc_zarr_uri, t2m_min_bc_ds, t2m_min_collection)
        else:
            t2m_min_bc_zarr_uri, t2m_min_collection = t2m_min_source
        
        # DOC: bias correcting maximum 2m temperature data
        
        LOGGER.info("\n### Start bias correction for maximum 2m temperature (t2m_max) data\n")
        
        t2m_max_zarr, t2m_max_collection = t2m_max_ingest_info
        t2m_max_source = self.ensure_collection_source(t2m_max_zarr, t2m_max_collection)
        if t2m_max_source is None:
            t2m_max_bc_ds = self.bias_correction_procedure(t2m_max_zarr, bc_var='tmx')
            t2m_max_bc_zarr_uri = self.upload_s3_bc_zarr(t2m_max_zarr, t2m_max_bc_ds)
            self.update_pygeoapi_config(t2m_max_bc_zarr_uri, t2m_max_bc_ds, t2m_max_collection)
        else:
            t2m_max_bc_zarr_uri, t2m_max_collection = t2m_max_source
        
        return (tp_bc_zarr_uri, tp_collection), (t2m_min_bc_zarr_uri, t2m_min_collection), (t2m_max_bc_zarr_uri, t2m_max_collection)
    
    
    # DOC: 4. Ensure collection source exists and update pygeoapi config if needed
    def ensure_collection_source(self, og_zarr_filename, collection_name):
        LOGGER.info(f"Check if bias corrected collection source exists for '{collection_name}' related to original zarr filename '{og_zarr_filename}'")
        
        bc_zarr_filename = self.og_to_bc_zarr_filename(og_zarr_filename)
        bc_zarr_uri = self.collection_data_uri(bc_zarr_filename)
        
        s3 = s3fs.S3FileSystem()
        config = utils.read_config(self.config_file)
        
        if s3.exists(bc_zarr_uri) and collection_name in config["resources"]:
            is_updated = config["resources"][collection_name]["providers"][0]["data"] == bc_zarr_uri
            LOGGER.info(f"Source '{bc_zarr_uri}' already exists. Collection '{collection_name}' is {'updated' if is_updated else 'not updated'} with bias corrected dataset '{bc_zarr_filename}'")
        
            if not is_updated:
                LOGGER.info(f"Updating collection with bias corrected dataset '{bc_zarr_filename}'")
                bc_ds_local_filepath = self.get_collection_dataset(bc_zarr_filename)
                bc_ds = xr.open_dataset(bc_ds_local_filepath)
                bc_ds.load()
                self.update_pygeoapi_config(bc_zarr_filename, bc_ds, collection_name)
                
            return (bc_zarr_uri, collection_name)
        
        return None    

    
    # DOC: 5. Bias correction procedure for total precipitation (tp) and 2m temperature (tmn, tmx)
    def bias_correction_procedure(self, og_zarr, bc_var):
        
        ds_local_filepath = self.get_collection_dataset(og_zarr)
        ds = xr.open_dataset(ds_local_filepath)
        ds.load()
        ds = ds.sel(time = ds.time.dt.month <= (ds.forecast_reference_time[0].dt.date + relativedelta(months=6)).item().month)   # DOC: we keep only forecast inside 6 months from the reference time month
        
        ds_vars = [v for v in list(ds.variables.keys()) if v not in list(ds.coords)]
        init_month = ds.forecast_reference_time[0].dt.month.item()
        
        diff_months = lambda date1, date2: (date2.year - date1.year) * 12 + (date2.month - date1.month)  # noqa: E731
        
        if bc_var == 'tp':
            preprocess = lambda og_ds: og_ds.diff(dim='time', n=1) * 1000   # DOC: convert to daily precipitation in mm from total precipitation in m
            biascorrect_model_observed = pd.read_csv(f'data/{self.living_lab}/bias_correction/bias_correction_observed_gamma.csv')
            biascorrect_model_forecast = pd.read_csv(f'data/{self.living_lab}/bias_correction/bias_correction_forecast_gamma.csv')
            get_rv = lambda fc_pars: stats.gamma(fc_pars['alpha'], loc=0., scale=fc_pars['scale'])
            get_rvo = lambda obs_pars: stats.gamma(obs_pars['alpha'], loc=0., scale=obs_pars['scale'])
            post_process = lambda bc_ds: xr.where(np.isinf(bc_ds), np.nan, bc_ds)
        elif bc_var in ['tmn', 'tmx']:
            preprocess = lambda og_ds: og_ds - 273.15   # DOC: convert from Kelvin to Celsius before correction
            biascorrect_model_observed = pd.read_csv(f'data/{self.living_lab}/bias_correction/bias_correction_observed_normal.csv')
            biascorrect_model_forecast = pd.read_csv(f'data/{self.living_lab}/bias_correction/bias_correction_forecast_normal.csv')
            get_rv = lambda fc_pars: stats.norm(fc_pars['mean'], fc_pars['stdev'])
            get_rvo = lambda obs_pars: stats.norm(obs_pars['mean'], obs_pars['stdev'])
            post_process = lambda bc_ds: xr.where(np.isinf(bc_ds), np.nan, bc_ds)
        else:
            # DOC: This should never be reached
            raise ProcessorExecuteError(f"Unsupported variable for bias correction: {bc_var}. Supported variables are 'tp', 'tmn', and 'tmx'.")
        
        
        def apply_bias_factors(bc_var, init_month, lead_month, ilead, lat, lon, fc_month_values):

            LOGGER.info(f"Appling bias factors for variable '{bc_var}' at lead month '{lead_month}' with ilead {ilead} for lat {lat} and lon {lon}")
            
            obs_pars = biascorrect_model_observed[
                (biascorrect_model_observed['initmonth']==init_month) &
                (biascorrect_model_observed['leadtime']==ilead) &
                (biascorrect_model_observed['lat']==lat) &
                (biascorrect_model_observed['lon']==lon) & 
                (biascorrect_model_observed['variable']==bc_var)
            ].iloc[0]
            
            fc_pars = biascorrect_model_forecast[
                (biascorrect_model_forecast['initmonth']==init_month) &
                (biascorrect_model_forecast['leadtime']==ilead) &
                (biascorrect_model_forecast['lat']==lat) &
                (biascorrect_model_forecast['lon']==lon) & 
                (biascorrect_model_forecast['variable']==bc_var)
            ].iloc[0]
            
            rv = get_rv(fc_pars)
            rvo = get_rvo(obs_pars)

            cdf = rv.cdf(fc_month_values)
            
            monthValuesN = stats.norm.ppf(cdf)        
            monthValuesB = rvo.ppf(stats.norm.cdf(monthValuesN))
            
            return monthValuesB.astype(np.float32)


        ds = preprocess(ds)
        
        for lead_month in sorted(set(ds.time.dt.strftime("%Y-%m").values)):
            
            ilead = diff_months(ds.forecast_reference_time[0].dt.date.item(), datetime.datetime.strptime(lead_month, "%Y-%m").date())
            
            for var in ds_vars[:2]:
                
                fc_month_values = ds.sel(
                    forecast_reference_time = ds.forecast_reference_time[0],
                    time = ds.time.dt.month == ds.forecast_reference_time[0].dt.month + ilead
                )[var].to_dataset()
                
                # DOC: Vectorized bias correction function: gains up to 5x speed compared to the nested looped version
                bc_values = xr.apply_ufunc(
                    apply_bias_factors,
                    bc_var, init_month, lead_month, ilead, fc_month_values.latitude, fc_month_values.longitude, fc_month_values[var],
                    vectorize=True,
                    input_core_dims=[[], [], [], [], [], [], ['time']],
                    output_core_dims=[['time']],
                    dask="parallelized"
                )
                
                ds[var].loc[dict(
                    forecast_reference_time = ds.forecast_reference_time[0],
                    time = ds.time.dt.month == ds.forecast_reference_time[0].dt.month + ilead
                )] = bc_values
                
        ds = post_process(ds)        
            
        return ds
    
    
    # DOC: 6. Upload bias-corrected dataset to S3
    def upload_s3_bc_zarr(self, og_zarr, bc_ds):
        bc_zarr = self.og_to_bc_zarr_filename(og_zarr)
        bc_zarr_uri = self.collection_data_uri(bc_zarr)
        
        LOGGER.info(f"Uploading bias-corrected dataset to S3: {bc_zarr_uri}")
        
        s3 = s3fs.S3FileSystem()
        s3_store = s3fs.S3Map(root=bc_zarr_uri, s3=s3, check=False)
        bc_ds.to_zarr(store=s3_store, consolidated=True, mode='w')
        return bc_zarr_uri
            
            
    # DOC: 7. Update pygeoapi configuration with the bias-corrected dataset    
    def update_pygeoapi_config(self, bc_zarr_uri, bc_ds, collection_name):
        
        LOGGER.info(f"Updating pygeoapi configuration for collection '{collection_name}' with bias-corrected dataset '{bc_zarr_uri}'")
        
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
            collection['providers'][0]['data'] = bc_zarr_uri
            s3_is_anon_access = s3_utils.is_s3_anon_access()
            if not s3_is_anon_access:
                alternate_root = bc_zarr_uri.split("s3://")[1]
                endpoint_url = os.environ.get(default="https://obs.eu-de.otc.t-systems.com", key='FSSPEC_S3_ENDPOINT_URL')
                collection['providers'][0]['options'] = {
                    's3': { 'anon': False, 'alternate_root': alternate_root, 'endpoint_url': endpoint_url, 'requester_pays': False }
                }
                
            config['resources'][collection_name] = collection
            
        utils.write_config(config_path=self.config_file, config_out=config)
        LOGGER.info(f"collection definition modified: '{config['resources'][collection_name]}'")
            
        
    def execute(self, data):
        mimetype = "application/json"

        outputs = {}
        try:
            
            # DOC: This process is intended to be run as a cron job, not manually.
            self.validate_parameters(data)
            
            tp_ingest_info, t2m_min_ingest_info, t2m_max_ingest_info = self.run_cds_ingestor_process()
            
            tp_bc_info, t2m_min_bc_info, t2m_max_bc_info = self.create_bias_corrected_collection(
                tp_ingest_info,
                t2m_min_ingest_info,
                t2m_max_ingest_info
            )
            
            outputs = {
                "status": "OK",
                
                "total_precipitation": {
                    "zarr_uri": tp_bc_info[0],
                    "collection": tp_bc_info[1]
                },
                
                "minimum_2m_temperature_in_the_last_24_hours": {
                    "zarr_uri": t2m_min_bc_info[0],
                    "collection": t2m_min_bc_info[1]
                },
                
                "maximum_2m_temperature_in_the_last_24_hours": {
                    "zarr_uri": t2m_max_bc_info[0],
                    "collection": t2m_max_bc_info[1]
                }
            }

        except Handle200Exception as err:
            outputs = {"status": err.status, "message": str(err)}
        except Exception as err:
            raise ProcessorExecuteError(str(err)) from err

        return mimetype, outputs
    

    def __repr__(self):
        return f"<GeorgiaCDSBiasCorrectionProcessProcessor> {self.name}"