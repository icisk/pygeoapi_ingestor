import json
import logging
import os
import shutil

import s3fs
import xarray as xr
from dotenv import find_dotenv, load_dotenv
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

LOGGER = logging.getLogger(__name__)

load_dotenv(find_dotenv())

PROCESS_METADATA = {
    "version": "0.2.0",
    "id": "cds-phenology-ingestor",
    "title": {
        "en": "creaf_historic",
    },
    "description": {
        "en": "creates geo-referenced tif files from cds agroclimatic indicators based on zarr file in online resource; also creates json metadata"
    },
    "jobControlOptions": ["sync-execute", "async-execute"],
    "keywords": ["ingestor process"],
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
        "zarr_source": {
            "title": "zarr_source",
            "description": "URI to the data, e.g. https://example.com/my-data.zarr",
            "schema": {"type": "string"},
        },
        "variable": {"title": "Variable", "description": "The name of the variable", "schema": {"type": "string"}},
        "token": {"title": "secret token", "description": "identify yourself", "schema": {"type": "string"}},
    },
    "outputs": {
        "id": {"title": "ID", "description": "The ID of the process execution", "schema": {"type": "string"}},
        "value": {"title": "Value", "description": "status msg", "schema": {"type": "string"}},
    },
    "example": {
        "inputs": {
            "zarr_source": "file:///data/creaf/precip_historic.zarr",
            "variable": "var_name",
            "token": "ABC123XYZ666",
        }
    },
}


class IngestorCDSPHENOLOGYProcessProcessor(BaseProcessor):
    """
    Ingestor Processor

    creates geo-referenced tif files from cds agroclimatic indicators based on zarr file in online resource; also creates json metadata

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
        self.config_file = os.environ.get(default="/pygeoapi/serv-config/local.config.yml", key="PYGEOAPI_SERV_CONFIG")
        self.title = "cds_phenology"
        self.otc_key = os.environ.get(key="FSSPEC_S3_KEY")
        self.otc_secret = os.environ.get(key="FSSPEC_S3_SECRET")
        self.otc_endpoint = os.environ.get(key="FSSPEC_S3_ENDPOINT_URL")
        self.alternate_root = None
        self.zarr_in = None
        # self.tif_base_out = None
        self.variable = None
        self.token = None
        self.bbox_spain = [-7.25, 36.25, -1.75, 39.25]
        self.base_path = "/tmp/pheno/"
        self.bucket = "s3://52n-i-cisk"

    def execute(self, data):
        mimetype = "application/json"

        self.zarr_in = data.get("zarr_in")
        self.token = data.get("token")
        self.variable = data.get("variable")

        LOGGER.debug("checking process inputs")
        if self.zarr_in is None:
            raise ProcessorExecuteError("Cannot process without a data path")
        if self.variable is None:
            raise ProcessorExecuteError("Cannot process without a variable")
        if self.token is None:
            raise ProcessorExecuteError("Identify yourself with valid token!")

        LOGGER.debug("checking token")
        if self.token != os.getenv("INT_API_TOKEN", "token"):
            # FIXME matching error?
            LOGGER.error("WRONG INTERNAL API TOKEN")
            raise ProcessorExecuteError("ACCESS DENIED wrong token")

        var_base_path = os.path.join(self.base_path, self.variable)
        os.makedirs(var_base_path, exist_ok=True)

        LOGGER.debug(f"reading zarr file '{self.zarr_in}'")
        ds = xr.open_zarr(self.zarr_in)
        x_min, y_min, x_max, y_max = self.bbox_spain

        LOGGER.debug(f"subsetting zarr file to bbox '{self.bbox_spain}")
        ds_spain = ds.sel(lon=slice(x_min, x_max), lat=slice(y_min, y_max))

        s3 = s3fs.S3FileSystem()

        metadata_file_name = (
            f"{self.bucket}/data-ingestor/spain/agro_indicator/{self.variable}/{self.variable}_metadata.json"
        )
        LOGGER.debug(f"writing metadata file '{metadata_file_name}'")
        time = dict(time=[str(t) for t in ds_spain["time"].values])
        time_data = json.dumps(time)
        with s3.open(metadata_file_name, "w") as file:
            file.write(time_data)

        count = len(ds_spain["time"].values)
        idx = 1
        LOGGER.debug(f"extracting time slices and generating '{count}' geotiff files")
        for t in ds_spain["time"].values:
            LOGGER.debug(f"processing time slice [{idx}/{count}]")
            file_name = f"""{self.variable}_{str(t).split("T")[0]}"""
            file_path = os.path.join(var_base_path, file_name)
            ds_spain[self.variable].sel(time=t).to_netcdf(f"{file_path}.nc")
            os.system(
                f"gdal_translate -a_ullr {x_min} {y_max} {x_max} {y_min} -a_srs EPSG:4326 {file_path}.nc {file_path}.tif"
            )
            idx += 1

        tif_file_paths = sorted(
            [os.path.join(var_base_path, f) for f in os.listdir(var_base_path) if f.endswith(".tif")]
        )

        count = len(tif_file_paths)
        idx = 1
        LOGGER.debug(f"uploading '{count}' geotiff files to bucket")
        for tif_file_path in tif_file_paths:
            LOGGER.debug(f"processing geotiff [{idx}/{count}]")
            tif_file = tif_file_path.split("/")[-1]
            s3_path = f"{self.bucket}/data-ingestor/spain/agro_indicator/{self.variable}/{tif_file}"

            with open(tif_file_path, "rb") as file:
                with s3.open(s3_path, "wb") as s3_file:
                    shutil.copyfileobj(file, s3_file)
            idx += 1

        outputs = {"id": "creaf_historic_ingestor", "value": "success"}
        return mimetype, outputs

    def __repr__(self):
        return f"<IngestorCDSPHENOLOGYProcessProcessor> {self.name}"
