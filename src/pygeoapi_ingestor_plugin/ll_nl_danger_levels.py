import json
import logging
import os

import numpy as np
import s3fs
import xarray as xr
from dotenv import find_dotenv, load_dotenv
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

LOGGER = logging.getLogger(__name__)

load_dotenv(find_dotenv())

PROCESS_METADATA = {
    "version": "0.2.0",
    "id": "danger-levels-creation",
    "title": {
        "en": "Danger Levels",
    },
    "description": {"en": "processes forecast data to identify drought threats"},
    "jobControlOptions": ["sync-execute", "async-execute"],
    "keywords": ["process"],
    "links": [
        {
            "type": "text/html",
            "rel": "about",
            "title": "information",
            "href": "https://example.org/process",
            "hreflang": "en-US",
        }
    ],
    "inputs": {
        "result_json": {"title": "result", "description": "The URL of the result", "schema": {"type": "string"}}
    },
    "outputs": {
        "id": {"title": "ID", "description": "The ID of the process execution", "schema": {"type": "string"}},
        "value": {
            "title": "Value",
            "description": "The URL of the Zarr file in the S3 bucket",
            "schema": {"type": "string"},
        },
    },
    "example": {"inputs": {"result_json": "www.j.son"}},
}


class DangerLevelProcessProcessor(BaseProcessor):
    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

        self.zarr_file = (
            "https://52n-i-cisk.obs.eu-de.otc.t-systems.com/data-ingestor/netherlands/knmi_obs_forecast.zarr/"
        )
        self.mask_file = "/pygeoapi/secondary_process_data/rijn_mask.nc"
        self.out_file = None
        self.otc_key = os.environ.get(key="FSSPEC_S3_KEY")
        self.otc_secret = os.environ.get(key="FSSPEC_S3_SECRET")
        self.otc_endpoint = os.environ.get(key="FSSPEC_S3_ENDPOINT_URL")
        self.bucket_name = os.environ.get(key="DEFAULT_BUCKET")

    def rijnland_precipitation_deficit(self):
        """
        1. Load a mask and a main dataset.
        2. Get sundays for each week.
        3. Extract week and month numbers for each week.
        4. Apply a mask to select specific regions (where mask is 666).
        5. Find the maximum value in the masked region for each week.
        6. Return arrays of week numbers, month numbers, and the weekly maximum values.
        """
        rj_mask = xr.open_dataset(self.mask_file)
        ds = xr.open_zarr(self.zarr_file)
        sundays = ds["time"].resample(time="1W").max()
        LOGGER.debug(f"sundays  : '{sundays}'")
        # Why is the last sunday skipped?
        sundays = sundays.time.values[:-1]
        LOGGER.debug(f"sundays  : '{sundays}'")
        week_numbers = np.array([t.astype("datetime64[W]").item().isocalendar()[1] for t in sundays])
        LOGGER.debug(f"week #s  : '{week_numbers}'")
        month_numbers = np.array([t.astype("datetime64[M]").item().month for t in sundays])
        LOGGER.debug(f"month #s : '{month_numbers}'")
        mask_vals = rj_mask["mask"].where(rj_mask["mask"] == 666).values
        LOGGER.debug(f"mask_vals: '{mask_vals}'")
        vals = [
            ds["p_def_q50"].sel(time=slice(t - np.timedelta64(6, "D"), t)).where(mask_vals).max().values
            for t in sundays
        ]
        LOGGER.debug(f"vals     : '{vals}'")

        return week_numbers, month_numbers, vals

    def categorize(self, value):
        LOGGER.debug(f"categorize: '{value}'")
        if value < 50:
            return "darkblue"
        if 50 <= value < 100:
            return "blue"
        if 100 <= value < 125:
            return "green"
        if 125 <= value < 150:
            return "yellow"
        if 150 <= value < 175:
            return "orange"
        if 175 <= value < 200:
            return "red"
        return "darkred"

    def execute(self, data):
        mimetype = "application/json"
        self.out_file = data.get("result_json")
        self.token = data.get("token")

        LOGGER.debug("checking process inputs")
        if self.out_file is None:
            raise ProcessorExecuteError("Cannot process without a json path")

        LOGGER.debug("checking token")
        if self.token != os.getenv("INT_API_TOKEN", "token"):
            # FIXME correct error?
            LOGGER.error("WRONG INTERNAL API TOKEN")
            raise ProcessorExecuteError("ACCESS DENIED wrong token")

        LOGGER.debug("start")
        rj_weeks, rj_month, rj_vals = self.rijnland_precipitation_deficit()
        rj_cat = [self.categorize(val) for val in rj_vals]
        rijn_dict = {int(item[0]): item[1] for item in [x for x in zip(rj_weeks, rj_cat)]}
        LOGGER.debug(f"week # with category: '{rijn_dict}'")

        json_data = json.dumps(rijn_dict)

        s3 = s3fs.S3FileSystem(
            key=os.environ.get("FSSPEC_S3_KEY"),
            secret=os.environ.get("FSSPEC_S3_SECRET"),
            client_kwargs={"endpoint_url": os.environ.get("FSSPEC_S3_ENDPOINT_URL")},
        )

        with s3.open(f"{self.bucket_name}/{self.out_file}", "w") as f:
            LOGGER.debug("start writing")
            f.write(json_data)

        outputs = {"id": "ll_nl_danger_levels", "value": self.out_file}
        LOGGER.debug("return")
        return mimetype, outputs

    def __repr__(self):
        return f"<DangerLevelProcessProcessor> {self.name}"
