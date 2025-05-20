import datetime
import json
import logging
import os
import re
import tempfile

import numpy as np
import pandas as pd
import s3fs
from filelock import FileLock
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

import pygeoapi_ingestor_plugin.utils as utils
import pygeoapi_ingestor_plugin.utils_s3 as s3_utils

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
        "en": "Georgia FTP Alazani Shaqriani Hydro Process",
    },
    "description": {
        "en": "Retrieves the Alazani Shaqriani Hydro data from the Georgia FTP server and stores in a pygeoapi geojson collection feeded by S3 file.",
    },
    "jobControlOptions": ["sync-execute", "async-execute"],
    "keywords": ["safer process"],
    "inputs": {
        "token": {"title": "secret token", "description": "identify yourself", "schema": {"type": "string"}},
        "date": {
            "title": "Requested date",
            "description": "Date of the requested data in YYYY-MM-DD format. It refers to the file name to be retrieved from S3. It must be before or equal to the current date.",
            "schema": {},
        },
        "debug": {"title": "Debug", "description": "Enable Debug mode", "schema": {}},
    },
    "outputs": {
        "status": {"title": "status", "description": "Staus of the process execution [OK or KO]", "schema": {}},
        "data": {"title": "data", "description": "The data output file"},
    },
    "example": {"inputs": {"debug": True, "token": "ABC123XYZ666", "date": "2025-05-08"}},
}


class Handle200Exception(Exception):
    OK = "OK"
    SKIPPED = "SKIPPED"
    DENIED = "DENIED"

    def __init__(self, status, message):
        self.status = status
        self.message = message
        super().__init__(self.message)


class GeorgiaFTPAlazaniShaqrianiHydroProcessor(BaseProcessor):
    """Georgia FTP Alazani Shaqriani Hydro Process class.

    This class implements the Georgia FTP Alazani Shaqriani Hydro process.
    It retrieves the Alazani Shaqriani Hydro data from the Georgia FTP server and stores it in a pygeoapi geojson collection fed by S3 file.
    """

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

        self.living_lab = (
            "georgia"  # DOC: This process is for Georgia living lab, specifically for Alazani Shaqriani station
        )
        self.station_id = 440  # DOC: This is the alazani-shaqriani station id
        self.station_coords = (
            45.58200868470981,
            41.98792251736575,
        )  # DOC: Coordinates of the Alazani Shaqriani station (lon,lat)

        self.config_file = os.environ.get(key="PYGEOAPI_SERV_CONFIG", default="/pygeoapi/serv-config/local.config.yml")

        self.collection_id = "georgia_alazani_shaqriani_hydro"
        self.collection_uri = utils.normpath(
            os.path.join(
                "s3://",
                os.environ.get("DEFAULT_BUCKET"),
                os.environ.get("DEFAULT_REMOTE_DIR"),
                self.living_lab,
                f"{self.collection_id}.geojson",
            )
        )

        self.process_temp_dir = os.path.join(tempfile.gettempdir(), "alazani_shaqriani_hydro_process")
        if not os.path.exists(self.process_temp_dir):
            os.makedirs(self.process_temp_dir, exist_ok=True)

    def validate_parameters(self, data):
        """Validate the input parameters."""

        token = data.get("token", None)
        cron_invocation = data.get("cron_invocation", False)

        if token is None:
            raise ProcessorExecuteError("You must provide an valid token")
        if token != os.getenv("INT_API_TOKEN", "token"):
            LOGGER.error(
                f"WRONG INTERNAL API TOKEN {token} ({type(token)}) != {os.getenv('INT_API_TOKEN', 'token')} ({type(os.getenv('INT_API_TOKEN', 'token'))})"
            )
            raise Handle200Exception(Handle200Exception.DENIED, "ACCESS DENIED: wrong token")

        date = data.get("date", None)

        if cron_invocation:
            date = datetime.datetime.now(datetime.timezone.utc).date()
        else:
            if date is None:
                raise ProcessorExecuteError("You must provide a date")
            if not isinstance(date, str):
                raise ProcessorExecuteError("Date must be a iso-format string in YYYY-MM-DD format")
            try:
                date = datetime.datetime.strptime(date, "%Y-%m-%d").date()
            except ValueError as e:
                raise ProcessorExecuteError("Date must be a iso-format string in YYYY-MM-DD format") from e
            if date > datetime.datetime.now(datetime.timezone.utc).date():
                raise ProcessorExecuteError("Date must be before or equal to the current date")

        return date

    def retrieve_file_from_ftp(self, date):
        """Retrieve the data from the FTP server."""

        def get_avaliable_files():
            avaliable_files = utils.list_ftp_files(
                server=os.environ.get("FTP_GEORGIA_HOST"),
                user=os.environ.get("FTP_GEORGIA_USER"),
                passwd=os.environ.get("FTP_GEORGIA_PASS"),
                source=os.environ.get("FTP_GEORGIA_DIR"),
            )
            return avaliable_files

        def find_requested_file():
            rgx = f"Alazani-Shaqriani-Hydro-.*{date.day}.*{date.month}.*{date.year}\.xlsx"
            avaliable_files = get_avaliable_files()
            if avaliable_files is not None and len(avaliable_files) > 0:
                for af in avaliable_files:
                    if re.search(rgx, af):
                        return af
            return None

        requested_file = find_requested_file()

        if requested_file is None:
            raise Handle200Exception(Handle200Exception.SKIPPED, "Requested file not found on FTP server")

        requested_local_file = os.path.join(self.process_temp_dir, os.path.basename(requested_file))
        utils.download_ftp_file(
            server=os.environ.get("FTP_GEORGIA_HOST"),
            user=os.environ.get("FTP_GEORGIA_USER"),
            passwd=os.environ.get("FTP_GEORGIA_PASS"),
            source=requested_file,
            out_path=requested_local_file,
        )

        return requested_local_file

    def update_collection(self, data_filename):
        def xlsx_to_df(xlsx_filename):
            df = pd.read_excel(xlsx_filename, engine="openpyxl", skiprows=[0, 1], usecols=[0, 1, 2])
            df.columns = ["date_time", "level", "Q"]
            df = df.sort_values(by="date_time", ascending=True).drop_duplicates(subset=["date_time"], keep="last")
            return df

        def df_to_geojson(dataframe):
            geojson_feature_collection = {
                "type": "FeatureCollection",
                "features": [
                    {
                        "id": self.station_id,
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": list(self.station_coords)},
                        "properties": {
                            "time": dataframe.date_time.apply(
                                lambda dt: dt if type(dt) is str else dt.isoformat(timespec="seconds")
                            ).to_list(),
                            "level": list(
                                map(
                                    lambda x: round(x, 4) if isinstance(x, float) and not np.isnan(x) else None,
                                    dataframe.level.to_list(),
                                )
                            ),
                            "Q": list(
                                map(
                                    lambda x: round(x, 4) if isinstance(x, float) and not np.isnan(x) else None,
                                    dataframe.Q.to_list(),
                                )
                            ),
                        },
                    }
                ],
                "metadata": {
                    "field": [
                        {"@name": "Q", "@alias": "river_discharge", "@unit": "m**3/s", "@type": "discharge"},
                        {"@name": "level", "@alias": "river_level", "@unit": "cm", "@type": "level"},
                    ]
                },
                "crs": {
                    "type": "name",
                    "properties": {
                        "name": "urn:ogc:def:crs:OGC:1.3:CRS84"  # REF: https://gist.github.com/sgillies/1233327 lines 256:271
                    },
                },
            }
            return geojson_feature_collection

        def update_s3_data(geojson_features_collection):
            s3 = s3fs.S3FileSystem()
            try:
                with s3.open(self.collection_uri, "r") as f:
                    existing_geojson = json.loads(f.read())

                df = (
                    pd.DataFrame(
                        {
                            "date_time": existing_geojson["features"][0]["properties"]["time"]
                            + geojson_feature_collection["features"][0]["properties"]["time"],
                            "level": existing_geojson["features"][0]["properties"]["level"]
                            + geojson_feature_collection["features"][0]["properties"]["level"],
                            "Q": existing_geojson["features"][0]["properties"]["Q"]
                            + geojson_feature_collection["features"][0]["properties"]["Q"],
                        }
                    )
                    .sort_values(by="date_time", ascending=True)
                    .drop_duplicates(subset=["date_time"], keep="last")
                )

                geojson_features_collection = df_to_geojson(df)

            except FileNotFoundError:
                pass

            finally:
                with s3.open(self.collection_uri, "w") as f:
                    json.dump(geojson_features_collection, f)
            return geojson_features_collection

        def update_pygeoapi_collection(geojson_features_collection):
            df = pd.DataFrame(
                {
                    "date_time": geojson_features_collection["features"][0]["properties"]["time"],
                    "level": geojson_features_collection["features"][0]["properties"]["level"],
                    "Q": geojson_features_collection["features"][0]["properties"]["Q"],
                }
            ).sort_values(by="date_time", ascending=True)

            min_dt = datetime.datetime.fromisoformat(df.date_time.min())
            max_dt = datetime.datetime.fromisoformat(df.date_time.max())

            lock = FileLock(f"{self.config_file}.lock", thread_local=False)
            with lock:
                config = utils.read_config(self.config_file)
                collection_definition = {
                    "type": "collection",
                    "title": self.collection_id,
                    "description": "River discharge for Alazani-Shaqriani station",
                    "keywords": ["river-dischage", "river-level", "Alazani-Shaqriani", "Georgia"],
                    "extents": {
                        "spatial": {"bbox": list(self.station_coords) + list(self.station_coords)},
                        "temporal": {"begin": min_dt, "end": max_dt},
                    },
                    "providers": [
                        {
                            "type": "feature",
                            "name": "S3GeoJSONProvider.S3GeoJSONProvider",
                            "data": self.collection_uri,
                            "id_field": "id",
                        }
                    ],
                }

                config["resources"][self.collection_id] = collection_definition

                s3_is_anon_access = s3_utils.is_s3_anon_access()
                endpoint_url = os.environ.get(
                    default="https://obs.eu-de.otc.t-systems.com", key="FSSPEC_S3_ENDPOINT_URL"
                )
                alternate_root = self.collection_uri.split("s3://")[1]
                if s3_is_anon_access:
                    config["resources"][self.collection_id]["providers"][0]["options"] = {
                        "s3": {"anon": True, "requester_pays": False}
                    }
                else:
                    config["resources"][self.collection_id]["providers"][0]["options"] = {
                        "s3": {
                            "anon": False,
                            "alternate_root": alternate_root,
                            "endpoint_url": endpoint_url,
                            "requester_pays": False,
                        }
                    }

            LOGGER.info(f"collection definition to add: '{config['resources'][self.collection_id]}'")
            utils.write_config(config_path=self.config_file, config_out=config)

        dataframe = xlsx_to_df(data_filename)
        geojson_feature_collection = df_to_geojson(dataframe)
        geojson_feature_collection = update_s3_data(geojson_feature_collection)
        update_pygeoapi_collection(geojson_feature_collection)

        return geojson_feature_collection

    def execute(self, data):
        mimetype = "application/json"

        outputs = {}
        try:
            # Validate request
            date = self.validate_parameters(data)

            # Get data from ftp server
            data_filename = self.retrieve_file_from_ftp(date)

            # Update Collection
            geojson_feature_collection = self.update_collection(data_filename)

            # Return OK + geojson added data
            outputs = {"status": "OK", "ingested_data": geojson_feature_collection}

        except Handle200Exception as err:
            outputs = {"status": err.status, "message": str(err)}
        except Exception as err:
            raise ProcessorExecuteError(str(err)) from err

        return mimetype, outputs

    def __repr__(self):
        return f"<GeorgiaFTPAlazaniShaqrianiHydroProcessor> {self.name}"
