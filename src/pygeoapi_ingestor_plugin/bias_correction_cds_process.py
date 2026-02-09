import datetime
import logging
import os
import tempfile
import requests

import cdsapi
import numpy as np
import pandas as pd
import s3fs
import xarray as xr
import json
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
from scipy.interpolate import splev

import pygeoapi_ingestor_plugin.utils as utils

# =================================================================
#
# Authors: Tommaso Redaelli <tommaso.redaelli@gecosistema.com>
#
# Copyright (c) 2023 Tommaso Redaelli
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
        "en": "Bias correction CDS procedure",
    },
    "description": {
        "en": "Bias correction procedure for ECMWF forecast temperature and precipitation data in Spain",
    },
    "jobControlOptions": ["sync-execute", "async-execute"],
    "keywords": ["safer process"],
    "inputs": {
        "token": {"title": "secret token", "description": "identify yourself", "schema": {"type": "string"}},
        "start_month": {
            "title": "init month for forecast data",
            "description": "Init month (this can't be more than actual month unless current date is after the 6th day of the month)",
            "schema": {},
        },
        "debug": {"title": "Debug", "description": "Enable Debug mode", "schema": {}},
    },
    "outputs": {
        "status": {"title": "status", "description": "Staus of the process execution [OK or KO]", "schema": {}},
        "data": {"title": "data", "description": "The data output file"},
    },
    "example": {"inputs": {"debug": True, "token": "ABC123XYZ666", "start_month": "2025-01"}},
}


class Handle200Exception(Exception):
    OK = "OK"
    SKIPPED = "SKIPPED"
    DENIED = "DENIED"

    def __init__(self, status, message):
        self.status = status
        self.message = message
        super().__init__(self.message)


class BiasCorrectionCDSProcessor(BaseProcessor):
    """
    Bias Correction CDS Processor
    """

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

        self.cds_client = cdsapi.Client(url="https://cds.climate.copernicus.eu/api", key=os.getenv("CDSAPI_KEY"))

        self.living_lab = "spain"  # INFO: This processor is specific for Spain living lab

        self.bucket_uri = utils.normpath(
            os.path.join(
                "s3://",
                os.environ.get("DEFAULT_BUCKET"),
                os.environ.get("DEFAULT_REMOTE_DIR"),
                self.living_lab,
                "bias_correction",
            )
        )

        self.process_temp_dir = os.path.join(tempfile.gettempdir(), "bias_correction_cds_process")
        if not os.path.exists(self.process_temp_dir):
            os.makedirs(self.process_temp_dir, exist_ok=True)

        self.process_data_dir = os.path.join(os.getcwd(), "bias_correction_process_data")

        self.metadata_uri = 'https://52n-i-cisk.obs.eu-de.otc.t-systems.com/data-ingestor/spain/bias_correction/metadata.json'
        self.s3_prefix = "s3://52n-i-cisk/"
        self.https_prefix = "https://52n-i-cisk.obs.eu-de.otc.t-systems.com/"

    def validate_parameters(self, data):
        """
        Validate request parameters
        """

        token = data.get("token", None)
        start_month = data.get("start_month", None)
        cron_invocation = data.get("cron_invocation", False)

        if token is None:
            raise ProcessorExecuteError("You must provide an valid token")
        if token != os.getenv("INT_API_TOKEN", "token"):
            LOGGER.error(
                f"WRONG INTERNAL API TOKEN {token} ({type(token)}) != {os.getenv('INT_API_TOKEN', 'token')} ({type(os.getenv('INT_API_TOKEN', 'token'))})"
            )
            raise Handle200Exception(Handle200Exception.DENIED, "ACCESS DENIED: wrong token")

        if start_month is None or cron_invocation is True:
            current_date = datetime.datetime.now()
            start_month = current_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        else:
            try:
                start_month = datetime.datetime.strptime(start_month, "%Y-%m")
            except ValueError as e:
                raise ProcessorExecuteError(
                    "Invalid start_month parameter. Must be a date in the format YYYY-MM"
                ) from e

        if start_month.strftime("%Y-%m") == datetime.datetime.now().strftime(
            "%Y-%m"
        ) and datetime.datetime.now() <= datetime.datetime.now().replace(day=6, hour=12, minute=0, second=0):
            raise Handle200Exception(
                Handle200Exception.SKIPPED, "period_of_interest in current month is available from day 6 at 12UTC"
            )

        if start_month.strftime("%Y-%m") > datetime.datetime.now().strftime("%Y-%m"):
            raise Handle200Exception(
                Handle200Exception.SKIPPED,
                f"period_of_interest of {start_month.strftime('%Y-%m')} will be available from day {start_month.strftime('%Y-%m')}-06 at 12UTC",
            )

        start_month = start_month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        def check_s3_path_exists(start_month):
            s3_t2m_uri, _ = self.build_s3_uri("t2m", start_month)
            s3_tp_uri, _ = self.build_s3_uri("tp", start_month)
            s3 = s3fs.S3FileSystem()
            is_tp_uploaded = s3_tp_uri.startswith("s3://") and s3.exists(s3_tp_uri)
            is_t2m_uploaded = s3_t2m_uri.startswith("s3://") and s3.exists(s3_t2m_uri)
            if is_tp_uploaded and is_t2m_uploaded:
                raise Handle200Exception(
                    Handle200Exception.OK, f"Path {s3_tp_uri} and {s3_t2m_uri} already exists in bucket"
                )

        check_s3_path_exists(start_month)

        return start_month

    def prepare_bias_procedure_data(self):
        def griddes_txt2netcdf(griddes_path):
            griddes_nc_filepath = griddes_path.replace(".txt", ".nc")
            if not os.path.exists(griddes_nc_filepath):
                with open(griddes_path, "r") as f:
                    lines = f.readlines()
                xline = [line for line in lines if "xvals" in line][0]
                yline = [line for line in lines if "yvals" in line][0]
                xvals = np.array([float(val) for val in xline.split("=")[1].strip().split()])
                yvals = np.array([float(val) for val in yline.split("=")[1].strip().split()])
                ds_grid = xr.Dataset(
                    {
                        "lon": (("y", "x"), np.reshape(xvals, (len(yvals), 1))),
                        "lat": (("y", "x"), np.reshape(yvals, (len(yvals), 1))),
                    }
                )
                ds_grid.to_netcdf(griddes_nc_filepath)
            return griddes_nc_filepath

        griddes_tp_path = os.path.join(self.process_data_dir, "grid_descriptions", "grid_description_tp.txt")
        self.grid_file_tp = griddes_txt2netcdf(griddes_tp_path)

        griddes_t2m_path = os.path.join(self.process_data_dir, "grid_descriptions", "grid_description_t2m.txt")
        self.grid_file_t2m = griddes_txt2netcdf(griddes_t2m_path)

        def load_parameters():
            params_dir = os.path.join(self.process_data_dir, "parameters")
            param_paths = [os.path.join(params_dir, pf) for pf in os.listdir(params_dir)]

            df_params = pd.DataFrame(param_paths, columns=["path"])
            df_params["var"] = df_params["path"].apply(lambda p: utils.juststem(p).split("__")[1])
            df_params["init_month"] = df_params["path"].apply(
                lambda p: int(utils.juststem(p).split("__")[2].split("_")[1])
            )
            df_params["lead_month"] = df_params["path"].apply(
                lambda p: int(utils.juststem(p).split("__")[3].split("_")[1])
            )
            df_params["param"] = df_params["path"].apply(lambda p: np.load(p)["arr_0"])
            df_params = df_params.drop(columns=["path"])
            return df_params

        LOGGER.debug("Loading bias correction parameters")
        self.df_params = load_parameters()

    def retrieve_cds_data(self, start_month):
        # REF: CDS data source: [ https://cds.climate.copernicus.eu/datasets/seasonal-original-single-levels?tab=overview ]

        cds_retrieved_filename = (
            f"ECMWF_seasonal_forecast__t2m-min_t2m-max_tot-prec__{start_month.year}-{start_month.month:02d}.nc"
        )
        cds_retrieved_filepath = os.path.join(self.process_temp_dir, cds_retrieved_filename)
        cds_dataset = "seasonal-original-single-levels"
        cds_request = {
            "originating_centre": "ecmwf",
            "system": "51",
            "variable": [
                "maximum_2m_temperature_in_the_last_24_hours",
                "minimum_2m_temperature_in_the_last_24_hours",
                "total_precipitation",
            ],
            "year": [f"{start_month.year}"],
            "month": [f"{start_month.month:02d}"],
            "day": ["01"],
            "leadtime_hour": [str(h) for h in range(24, 24 * 215 + 24, 24)],
            "area": [
                40,  # N
                -8,  # W
                36,  # S
                -1,  # E
            ],
            "data_format": "netcdf",
        }

        LOGGER.debug(f"Retrieving data from CDS: {cds_dataset} - {start_month.year}-{start_month.month:02d}")
        self.cds_client.retrieve(cds_dataset, cds_request, cds_retrieved_filepath)
        return cds_retrieved_filepath

    def preprocess_cds_dataset(self, cds_dataset):
        def preprocess_tp(cds_dataset):
            ds_tp = xr.Dataset(
                {"tp": (["r", "ref_time", "time", "lat", "lon"], cds_dataset.tp.values)},
                coords={
                    "r": cds_dataset.number.values,
                    "ref_time": cds_dataset.forecast_reference_time.values,
                    "time": cds_dataset.valid_time.values,
                    "lat": cds_dataset.latitude.values,
                    "lon": cds_dataset.longitude.values,
                },
            )
            ds_tp = ds_tp.sortby(["time", "lat", "lon"])

            ds_grid_tp = xr.open_dataset(self.grid_file_tp)
            ds_tp = ds_tp.interp(lat=ds_grid_tp.lat, lon=ds_grid_tp.lon, method="linear")

            ds_tp["tp"] = ds_tp["tp"] * 1e3

            return ds_tp

        def preprocess_t2m(cds_dataset):
            ds_t2m = xr.Dataset(
                {
                    "t2m_min": (["r", "ref_time", "time", "lat", "lon"], cds_dataset.mn2t24.values),
                    "t2m_max": (["r", "ref_time", "time", "lat", "lon"], cds_dataset.mx2t24.values),
                },
                coords={
                    "r": cds_dataset.number.values,
                    "ref_time": cds_dataset.forecast_reference_time.values,
                    "time": cds_dataset.valid_time.values,
                    "lat": cds_dataset.latitude.values,
                    "lon": cds_dataset.longitude.values,
                },
            )
            ds_t2m = ds_t2m.sortby(["time", "lat", "lon"])

            ds_grid_t2m = xr.open_dataset(self.grid_file_t2m)
            ds_t2m = ds_t2m.interp(lat=ds_grid_t2m.lat, lon=ds_grid_t2m.lon, method="linear")

            ds_t2m["t2m_min"] = ds_t2m["t2m_min"] - 273.15
            ds_t2m["t2m_max"] = ds_t2m["t2m_max"] - 273.15
            ds_t2m["t2m_range"] = ds_t2m["t2m_max"] - ds_t2m["t2m_min"]

            ds_t2m = ds_t2m.drop_vars("t2m_max")

            return ds_t2m

        LOGGER.debug("Preprocessing CDS dataset")
        ds_tp = preprocess_tp(cds_dataset)
        ds_t2m = preprocess_t2m(cds_dataset)
        return ds_tp, ds_t2m

    def bias_correction(self, ds, ds_varname):
        def correction_procedure(varname, r, ref_time, lead_month, data, st_idx):
            def load_param_file(varname, init_month, lead_month):
                found_params = self.df_params.query(
                    f'var == "{varname}" and init_month == {init_month} and lead_month == {lead_month}'
                )
                if found_params.empty:
                    return None
                return found_params.iloc[0].param

            param = load_param_file(varname, ref_time, lead_month)

            d = data.copy()  # Reference data ( 1-r, 1-ref_time, N-days-lead_month, 1-st_idx )

            if param is not None:
                ind_degree = 1  # Index of degree of spline fit
                ind_no_knots = 2  # Index where the number of spline knots is saved
                ind_knots = 3  # Index where the knots start

                n = int(param[st_idx, 0, ind_no_knots])  # number of knots

                ind_coeffs = ind_knots + n  # indices range for coefficients of spline

                ind_dmin = ind_coeffs + n  # Indices where minimum value for spline fit is stored
                ind_dmax = ind_dmin + 1  # Indices where maximum value for spline fit is stored
                # ind_ssr_threshold = ind_dmax + 1                    # Index where the wet-day threshold is stored # used in never used variable

                k = int(param[st_idx, 0, ind_degree])
                t = param[st_idx, 0, ind_knots:ind_coeffs]
                c = param[st_idx, 0, ind_coeffs:ind_dmin]
                dmin = param[st_idx, 0, ind_dmin]
                dmax = param[st_idx, 0, ind_dmax]
                tck = (t, c, k)  # tuple contains the input parameters to the spline method

                tmin = t[k]
                tmax = t[-k]
                # ssr_threshold = param[st_idx,0,ind_ssr_threshold]   # ???: This is never used
                n_ts = len(d)  # Number of time steps

                for ts in range(
                    0, n_ts
                ):  # Below or about the spline limits, a constant biasadjustment value is assumed (dmin or dmax)
                    under_range = d[ts] < tmin
                    over_range = d[ts] > tmax
                    if under_range:
                        d[ts] = d[ts] + dmin
                    elif over_range:
                        d[ts] = d[ts] + dmax
                    else:
                        d[ts] = splev(d[ts], tck, ext=2)

            return d

        ds_varname_adj = f"{ds_varname}_adj"
        ds[ds_varname_adj] = ds[ds_varname].copy()

        for lm in list(set(ds.time.dt.month.values)):
            ds_lm = ds.sel(time=ds.time.dt.month == lm)
            ds_lm[ds_varname_adj] = xr.apply_ufunc(
                correction_procedure,
                ds_varname,
                ds_lm.r,
                ds_lm.ref_time.dt.month,
                lm,
                ds_lm[ds_varname],
                ds_lm.y,
                vectorize=True,
                input_core_dims=[[], [], [], [], ["time"], []],
                output_core_dims=[["time"]],
                dask="parallelized",
            )
            ds_lm[ds_varname_adj] = ds_lm[ds_varname_adj].transpose("r", "ref_time", "time", "y", "x")
            ds[ds_varname_adj].loc[dict(time=ds.time.dt.month == lm)] = ds_lm[ds_varname_adj]

        return ds

    def bias_correction_tp(self, ds_tp):
        LOGGER.debug("Bias correcting total precipitation")
        ds_tp = self.bias_correction(ds_tp, "tp")
        ds_tp = ds_tp.drop_vars("tp")
        ds_tp["tp_adj"] = xr.where(ds_tp["tp_adj"] < 0, 0, ds_tp["tp_adj"])
        return ds_tp

    def bias_correction_t2m(self, ds_t2m):
        LOGGER.debug("Bias correcting temperature")
        ds_t2m = self.bias_correction(ds_t2m, "t2m_min")
        ds_t2m = self.bias_correction(ds_t2m, "t2m_range")
        ds_t2m = ds_t2m.drop_vars(["t2m_min", "t2m_range"])
        ds_t2m["t2m_max_adj"] = ds_t2m["t2m_min_adj"] + ds_t2m["t2m_range_adj"]
        ds_t2m["t2m_avg_adj"] = (ds_t2m["t2m_min_adj"] + ds_t2m["t2m_max_adj"]) / 2
        return ds_t2m

    def build_s3_uri(self, var_name, start_month):
        dataset_part = "ECMWF_51"
        variable_part = f"{var_name}-bias_corrected"
        date_part = start_month.strftime("%Y-%m")
        filename = f"{dataset_part}__{variable_part}__{date_part}.nc"
        s3_uri = utils.normpath(os.path.join(self.bucket_uri, filename))
        return s3_uri, filename

    def save_dataset_to_s3(self, dataset, ds_varname):
        ds_start_month = dataset.time.min().dt.date.item()
        s3_uri, filename = self.build_s3_uri(ds_varname, ds_start_month)
        local_filepath = os.path.join(self.process_temp_dir, filename)
        s3 = s3fs.S3FileSystem()

        # Save dataset locally first
        dataset.to_netcdf(local_filepath)

        LOGGER.debug(f"Uploading to {s3_uri}")

        # Upload to S3
        with open(local_filepath, "rb") as f:
            with s3.open(s3_uri, "wb") as s3_f:
                s3_f.write(f.read())

        return s3_uri

    def create_metadata(self, start_month, s3_tp_adj_uri, s3_t2m_adj_uri):
        #change s3 uri to https uri
        https_tp_uri = s3_tp_adj_uri.replace(self.s3_prefix, self.https_prefix)
        https_t2m_uri = s3_t2m_adj_uri.replace(self.s3_prefix, self.https_prefix)

        #read metadata & add entry
        res = requests.get(self.metadata_uri)
        if res.status_code == 200:
            meta = json.loads(res.content)
            meta[start_month.strftime("%Y-%m")] = {
                    "tp": https_tp_uri,
                    "t2m": https_t2m_uri
                }
        else:
            meta = {
                start_month.strftime("%Y-%m"): {
                    "tp": https_tp_uri,
                    "t2m": https_t2m_uri
                }
            }

        #write to bucket
        s3 = s3fs.S3FileSystem()
        with s3.open(os.path.join(self.bucket_uri, 'metadata.json'), 'w') as f:
            json.dump(meta, f)


    def execute(self, data):
        mimetype = "application/json"

        outputs = {}
        try:
            # Validate request + check if already done
            start_month = self.validate_parameters(data)

            # Load bias-correction parameters
            self.prepare_bias_procedure_data()

            # Get data from CDS
            cds_dataset_filepath = self.retrieve_cds_data(start_month)
            cds_dataset = xr.open_dataset(cds_dataset_filepath)

            # Preprocess operations
            ds_tp, ds_t2m = self.preprocess_cds_dataset(cds_dataset)

            # !!!: Just to test in reasonable time
            # ds_tp, ds_t2m = ds_tp.isel(r=[0,1]), ds_t2m.isel(r=[0,1])

            # Bias correction operations
            ds_tp_adj = self.bias_correction_tp(ds_tp)
            ds_t2m_adj = self.bias_correction_t2m(ds_t2m)

            # Save output
            s3_tp_adj_uri = self.save_dataset_to_s3(ds_tp_adj, "tp")
            s3_t2m_adj_uri = self.save_dataset_to_s3(ds_t2m_adj, "t2m")

            self.create_metadata(start_month, s3_tp_adj_uri, s3_t2m_adj_uri)

            outputs = {"status": "OK", "s3_uris": {"tp": s3_tp_adj_uri, "t2m": s3_t2m_adj_uri}}

        except Handle200Exception as err:
            outputs = {"status": err.status, "message": str(err)}
        except Exception as err:
            raise ProcessorExecuteError(str(err)) from err

        return mimetype, outputs

    def __repr__(self):
        return f"<BiasCorrectionCDSProcessor> {self.name}"
