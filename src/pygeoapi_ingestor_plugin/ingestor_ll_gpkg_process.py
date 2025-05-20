import logging
import os

import geopandas as gpd
import yaml
from dotenv import find_dotenv, load_dotenv
from filelock import FileLock
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
from sqlalchemy import create_engine

from .utils import download_source

LOGGER = logging.getLogger(__name__)

load_dotenv(find_dotenv())

PROCESS_METADATA = {
    "version": "0.2.0",
    "id": "ll_gpkg_ingestor",
    "title": {
        "en": "ll_gpkg",
    },
    "description": {"en": "creates collections in postgis db from gpkg layers"},
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
        "data_url": {"title": "data url", "description": "The URL of the gpkg", "schema": {"type": "string"}},
        "ll": {"title": "living lab name", "description": "ll prefix", "schema": {"type": "string"}},
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
    "example": {"inputs": {"data_url": "s3://example/target/ll.gpkg", "variable": "ll_spain", "token": "ABC123XYZ666"}},
}


class IngestorLLGPKGProcessProcessor(BaseProcessor):
    """
    Ingestor Processor

    puts gpkg in db

    Resource Requirements:
    - CPU: 3
    - RAM: 12G
    """

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition

        :returns: pygeoapi.process.ingestor_process.IngestorLLGPKGProcessProcessor
        """

        super().__init__(processor_def, PROCESS_METADATA)
        self.config_file = os.environ.get(default="/pygeoapi/serv-config/local.config.yml", key="PYGEOAPI_SERV_CONFIG")
        self.title = None
        self.ll = None
        self.db_user = os.environ.get(key="DB_USER", default="postgres")
        self.db_password = os.environ.get(key="DB_PASSWORD", default="password")
        self.db_host = os.environ.get(key="DB_HOST", default="localhost")
        self.db_port = int(os.environ.get(key="DB_PORT", default="5432"))
        self.db_database = os.environ.get(key="DB_DATABASE", default="postgres")
        self.data_url = None
        self.data_path = None

    def read_config(self):
        with open(self.config_file, "r") as file:
            LOGGER.debug("read config")
            return yaml.safe_load(file)

    def write_config(self, new_config):
        with open(self.config_file, "w") as outfile:
            yaml.dump(new_config, outfile, default_flow_style=False)
        LOGGER.debug("updated config")

    def update_config(self, var):
        # min_x, min_y, max_x, max_y = [float(val) for val in self.data.total_bounds]

        # THIS MUST BE THE SAME IN ALL PROCESSES UPDATING THE SERV CONFIG
        lock = FileLock(f"{self.config_file}.lock")

        with lock:
            config = self.read_config()
            config["resources"][f"{self.ll}_{var}"] = {
                "type": "collection",
                "title": f"{self.ll}_{var}",
                "description": f"historic station data of {var}",
                "keywords": ["country"],
                "extents": {
                    "spatial": {"bbox": [-180, -90, 180, 90], "crs": "http://www.opengis.net/def/crs/EPSG/0/4326"},
                },
                "providers": [
                    {
                        "type": "feature",
                        "name": "PostgreSQL",
                        "data": {
                            "host": self.db_host,
                            "port": self.db_port,
                            "dbname": self.db_database,
                            "user": self.db_user,
                            "password": self.db_password,
                            "search_path": ["public"],
                        },
                        "id_field": "index",
                        "table": f"{self.ll}_{var}",
                        "geom_field": "geometry",
                    }
                ],
            }

            self.write_config(config)

    def check_config_if_ds_is_collection(self):
        config = self.read_config()
        return self.title in config["resources"]

    def execute(self, data):
        mimetype = "application/json"

        self.data_url = data.get("data_url")
        self.ll = data.get("ll")
        self.token = data.get("token")

        LOGGER.debug("checking process inputs")
        if self.data_url is None:
            raise ProcessorExecuteError("Cannot process without a zarr path")
        if self.token is None:
            raise ProcessorExecuteError("Identify yourself with valid token!")
        if self.ll is None:
            raise ProcessorExecuteError("No living lab specified")

        LOGGER.debug("checking token")
        if self.token != os.getenv("INT_API_TOKEN", "token"):
            # FIXME matching error?
            LOGGER.error("WRONG INTERNAL API TOKEN")
            raise ProcessorExecuteError("ACCESS DENIED wrong token")

        self.data_path = download_source(self.data_url)

        layers = gpd.list_layers(self.data_path)
        layer_dict = {}
        for layer in layers["name"]:
            print(layer)
            layer_dict[layer] = gpd.read_file(self.data_path, layer=layer)

        engine = create_engine(
            f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_database}"
        )

        for key in layer_dict.keys():
            layer_dict[key].to_postgis(f"{self.ll}_{key}", engine, if_exists="replace", index=True)
            self.update_config(key)

        outputs = {"id": "ll_gpkg_ingestor", "value": "done"}

        return mimetype, outputs

    def __repr__(self):
        return f"<IngestorLLGPKGProcessProcessor> {self.name}"
