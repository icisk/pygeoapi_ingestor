import logging
import os

import fsspec
import numpy as np
import s3fs
import tifffile as tiff
import xarray as xr
import yaml
import glob
from dotenv import find_dotenv, load_dotenv
from datetime import datetime
from filelock import FileLock
from osgeo import gdal
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from .utils import cleanup_data_temp, download_source

logger = logging.getLogger(__name__)

load_dotenv(find_dotenv())

PROCESS_METADATA = {
    "version": "0.2.0",
    "id": "creaf_oliveoil_ingestor",
    "title": {
        "en": "creaf_oliveoil",
    },
    "description": {"en": "joins tiff-data and saves it to zarr format and uploads it to s3 bucket"},
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
        "data_source": {
            "title": "data_source",
            "description": "URL to the data, e.g. https://example.com/my-data.zip",
            "schema": {"type": "string"},
        },
        "zarr_out": {
            "title": "Zarr Output",
            "description": "The URL of the Zarr file in the S3 bucket",
            "schema": {"type": "string"},
        },
        "token": {"title": "secret token", "description": "identify yourself", "schema": {"type": "string"}},
    },
    "outputs": {
        "id": {"title": "ID", "description": "The ID of the process execution", "schema": {"type": "string"}},
        "value": {
            "title": "Value",
            "description": "The URL of the Zarr file in the S3 bucket",
            "schema": {"type": "string"},
        },
    },
    "example": {
        "inputs": {
            "data_source": "file:///data/creaf/precip_forecast",
            "zarr_out": "s3://example/target/bucket.zarr",
            "token": "ABC123XYZ666",
        }
    },
}


def get_pixel_centroids(file_path):
    ds = gdal.Open(file_path)
    gt = ds.GetGeoTransform()
    x_size, y_size = ds.RasterXSize, ds.RasterYSize

    x_centroids = [gt[0] + gt[1] * (x + 0.5) for x in range(x_size)]
    y_centroids = [gt[3] + gt[5] * (y + 0.5) for y in range(y_size)]

    return x_centroids, y_centroids


def get_variable_name(filename):
    """
    Extracts the variable name from a filename, handling both single and
    multiple-part variable names.
    100% AI
    """
    # Remove the file extension and split the filename by '_'
    parts = os.path.splitext(filename)[0].split("_")

    # The variable name starts after the fourth underscore (index 4)
    # The number of parts after this depends on the variable name format

    # Check if the part at index 4 is a two-letter abbreviation followed by an underscore
    # This pattern indicates a two-part variable name like 'pc_50' or 'q_90'
    if len(parts) > 5 and len(parts[4]) == 2 and parts[4].isalpha() and parts[5].isdigit():
        return f"{parts[4]}_{parts[5]}"
    else:
        # Otherwise, the variable name is a single part
        return parts[4]


def project_tiff_to_ds(path):
    files = [os.path.join(path, f) for f in sorted(os.listdir(path)) if f.endswith(".tif") or f.endswith(".tiff")]
    file_names = [os.path.splitext(f)[0] for f in sorted(os.listdir(path))]

    variable = 'yield'
    years = np.array([str(i) for i in range(2002, 2023)], dtype='datetime64[Y]')

    arrays = [tiff.imread(file) for file in files]
    stacked = np.stack(arrays, axis=0)
    da = xr.DataArray(stacked, dims=["time", "latitude", "longitude"], name=variable)

    x, y = get_pixel_centroids(files[0])

    ds = xr.Dataset(
        {variable: (["time", "latitude", "longitude"], da.data)},
        coords={"time": years, "latitude": y, "longitude": x},
        attrs={"long_name": "yield", "units": "kg/ha"},

    )

    return ds


class IngestorCREAFOLIVEOILProcessProcessor(BaseProcessor):
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
        self.config_file = os.environ.get(default="/pygeoapi/serv-config/local.config.yml", key="PYGEOAPI_SERV_CONFIG")
        self.title = "creaf_oliveoil"
        self.otc_key = os.environ.get(key="FSSPEC_S3_KEY")
        self.otc_secret = os.environ.get(key="FSSPEC_S3_SECRET")
        self.otc_endpoint = os.environ.get(key="FSSPEC_S3_ENDPOINT_URL")
        self.alternate_root = None
        self.data_path = None
        self.data_source = None
        self.zarr_out = None
        self.token = None

    def read_config(self):
        with open(self.config_file, "r") as file:
            logger.info("read config")
            return yaml.safe_load(file)

    def write_config(self, new_config):
        with open(self.config_file, "w") as outfile:
            yaml.dump(new_config, outfile, default_flow_style=False)
        logger.info("updated config")

    def get_data_from_cloud(self):
        mapper = fsspec.get_mapper(
            self.zarr_out,
            alternate_root=self.alternate_root,
            endpoint_url=self.otc_endpoint,
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
        lock = FileLock(f"{self.config_file}.lock", thread_local=False)

        with lock:
            config = self.read_config()
            config["resources"][f"{self.title}"] = {
                "type": "collection",
                "title": f"{self.title}",
                "description": f"creaf_oliveoil of",
                "keywords": ["country"],
                "extents": {
                    "spatial": {
                        "bbox": [min_x, min_y, max_x, max_y],
                        "crs": "http://www.opengis.net/def/crs/EPSG/0/25830",
                    },
                },
                "providers": [
                    {
                        "type": "edr",
                        "name": "xarray-edr",
                        "data": self.zarr_out,
                        "x_field": "longitude",
                        "y_field": "latitude",
                        "time_field": "time",
                        "format": {"name": "zarr", "mimetype": "application/zip"},
                        # FIXME: check s3->anon: True should be working; maybe s3 vs http api
                        "options": {
                            "s3": {
                                "anon": False,
                                "alternate_root": self.alternate_root,
                                "endpoint_url": self.otc_endpoint,
                                "requester_pays": False,
                            }
                        },
                    }
                ],
            }

            self.write_config(config)

    def check_config_if_ds_is_collection(self):
        config = self.read_config()
        return self.title in config["resources"]

    def execute(self, data):
        mimetype = "application/json"
        #
        self.data_source = data.get("data_source")
        self.zarr_out = data.get("zarr_out")
        self.token = data.get("token")
        self.alternate_root = self.zarr_out.split("s3://")[1]

        if self.data_source is None:
            raise ProcessorExecuteError("Cannot process without a data path")
        if self.zarr_out is None or not self.zarr_out.startswith("s3://"):
            raise ProcessorExecuteError("Cannot process without a zarr path")

        if self.token is None:
            raise ProcessorExecuteError("Identify yourself with valid token!")

        if self.token != os.getenv("INT_API_TOKEN", "token"):
            # TODO Is this the correct error to return? Does this result in a BadRequest error?
            logger.info("wrong internal API token received")
            raise ProcessorExecuteError("ACCESS DENIED wrong token")

        # #adapt continous creation
        # #gets date string eg "https://52n-i-cisk.obs.eu-de.otc.t-systems.com/tif/LL_Spain/forecast/seasonal_forecast_precip_2025_01.zip"
        # self.date_stamp = self.data_source.split('.zip')[:-7]
        # #adds date string to zarr filename
        # self.zarr_out = self.zarr_out.replace('.zarr', f'{self.date_stamp}.zarr')

        if self.zarr_out and self.zarr_out.startswith("s3://"):
            s3 = s3fs.S3FileSystem()
            if s3.exists(self.zarr_out):
                if self.title in self.read_config()["resources"]:
                    msg = f"Path {self.zarr_out} already exists in bucket and config"
                else:
                    self.update_config()
                    msg = f"Path {self.zarr_out} already exists updates config at '{self.config_file}'"

                logger.info(msg)
                return mimetype, {"id": "creaf_oliveoil_ingestor", "value": msg}

        logger.debug(f"""{self.data_source}, {self.zarr_out}""")
        store = s3fs.S3Map(root=self.zarr_out, s3=s3, check=False)
        data_path = download_source(self.data_source)
        tiff_da = project_tiff_to_ds(data_path)
        tiff_da.to_zarr(store=store, consolidated=True, mode="w")

        self.update_config()
        cleanup_data_temp()

        outputs = {"id": "creaf_oliveoil_ingestor", "value": self.zarr_out}

        return mimetype, outputs

    def __repr__(self):
        return f"<IngestorCREAFOLIVEOILProcessProcessor> {self.name}"
