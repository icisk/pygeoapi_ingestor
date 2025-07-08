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
import datetime
import json
import logging
import os
import tempfile

import geopandas as gpd
import pandas as pd
import requests
import s3fs
from filelock import FileLock
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
from shapely.geometry import Point

import pygeoapi_ingestor_plugin.utils as utils
import pygeoapi_ingestor_plugin.utils_s3 as s3_utils

LOGGER = logging.getLogger(__name__)


#: Process metadata and description
PROCESS_METADATA = {
    "version": "0.2.0",
    "id": "safer-process",
    "title": {
        "en": "River Discharge ARPAE process",
    },
    "description": {
        "en": "Retrieve river discharge data from ARPAE and update a relative S3 geojson collection",
    },
    "jobControlOptions": ["sync-execute", "async-execute"],
    "keywords": ["safer process"],
    "inputs": {
        "token": {"title": "secret token", "description": "identify yourself", "schema": {"type": "string"}},
        "time_start": {
            "title": "time start",
            "description": "Start datetime of data to be collected. Default is null (all data until end_time)",
            "schema": {},
        },
        "time_end": {
            "title": "time end",
            "description": "End datetime of data to be collected. Default is null (all data from start_time)",
            "schema": {},
        },
        "stations": {
            "title": "stations",
            "description": "List of stations to be collected. Default is null (all stations)",
            "schema": {"type": "array", "items": {"type": "string"}},
        },
        "debug": {"title": "Debug", "description": "Enable Debug mode", "schema": {}},
    },
    "outputs": {
        "status": {"title": "status", "description": "Status of the process execution [OK or KO]", "schema": {}},
        "data": {"title": "data", "description": "The data output file"},
    },
    "example": {
        "inputs": {
            "debug": True,
            "token": "ABC123XYZ666",
            "time_start": "2025-04-01T00:00:00Z",
            "time_end": "2025-04-06T23:59:59Z",
            "stations": ["Lugo", "Ponteveggia"],
        }
    },
}


class Handle200Exception(Exception):
    OK = "OK"
    ERROR = "ERROR"
    DENIED = "DENIED"

    def __init__(self, status, message):
        self.status = status
        self.message = message
        super().__init__(self.message)


class RerRiverDischargeArpaeProcessor(BaseProcessor):
    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

        self.living_lab = "emilia-romagna"  # INFO: This processor is specific for Emilia-Romagna (Italy) living lab

        self.arpae_service_url = "https://dati-simc.arpae.it/opendata/osservati/meteo/realtime/realtime.jsonl"

        self.config_file = os.environ.get(key="PYGEOAPI_SERV_CONFIG", default="/pygeoapi/serv-config/local.config.yml")

        self.collection_id = "emilia-romagna_arpae-stations_river-discharge"
        self.collection_uri = utils.normpath(
            os.path.join(
                "s3://",
                os.environ.get("DEFAULT_BUCKET"),
                os.environ.get("DEFAULT_REMOTE_DIR"),
                self.living_lab,
                "river-discharge",
                "arpae-stations_river-discharge.geojson",
            )
        )

        self.process_temp_dir = os.path.join(tempfile.gettempdir(), "rer_river_discharge_arpae_process")
        if not os.path.exists(self.process_temp_dir):
            os.makedirs(self.process_temp_dir, exist_ok=True)

        self.info_dict_code2name = {
            "B01019": "station_name",
            "B01194": "report_mnemonic",
            "B07030": "station_height",
            "B05001": "latitude",
            "B06001": "longitude",
            "B04001": "year",
            "B04002": "month",
            "B04003": "day",
            "B04004": "hour",
            "B04005": "minute",
            "B04006": "second",
        }
        self.info_dict_name2code = {v: k for k, v in self.info_dict_code2name.items()}

        self.var_dict_code2name = {
            "B13215": "river_level",
        }
        self.var_dict_name2code = {v: k for k, v in self.var_dict_code2name.items()}

        self.station_ids = {"Ponte Veggia": 100, "Lugo": 101}

    def validate_parameters(self, data):
        token = data.get("token", None)
        time_start = data.get("time_start", None)
        time_end = data.get("time_end", None)
        stations = data.get("stations", None)
        cron_invocation = data.get("cron_invocation", False)

        if token is None:
            raise ProcessorExecuteError("You must provide an valid token")
        if token != os.getenv("INT_API_TOKEN", "token"):
            LOGGER.error(
                f"WRONG INTERNAL API TOKEN {token} ({type(token)}) != {os.getenv('INT_API_TOKEN', 'token')} ({type(os.getenv('INT_API_TOKEN', 'token'))})"
            )
            raise Handle200Exception(Handle200Exception.DENIED, "ACCESS DENIED: wrong token")

        if cron_invocation:
            time_start = None
            time_end = None
            stations = list(self.station_ids.keys())
        else:
            if time_start is not None:
                try:
                    time_start = datetime.datetime.fromisoformat(time_start)
                except Exception as err:
                    raise ProcessorExecuteError(f"Invalid time_start: {str(err)}. Must be in ISO format") from err
            if time_end is not None:
                try:
                    time_end = datetime.datetime.fromisoformat(time_end)
                except Exception as err:
                    raise ProcessorExecuteError(f"Invalid time_end: {str(err)}. Must be in ISO format") from err
            if time_start is not None and time_end is not None:
                if time_start >= time_end:
                    raise ProcessorExecuteError("time_start must be before time_end")

            if stations is not None:
                if not isinstance(stations, list):
                    raise ProcessorExecuteError("stations must be a list")
                if len(stations) == 0:
                    raise ProcessorExecuteError("if not null, stations must contain at least one element")
                if any([not isinstance(s, str) for s in stations]):
                    raise ProcessorExecuteError("stations must be a list of strings")

        return time_start, time_end, stations

    def get_arpae_data(self):
        def request_arpae_data():
            out_file = os.path.join(self.process_temp_dir, "arpae_realtime_data.json")
            try:
                response = requests.get(self.arpae_service_url)
                response.raise_for_status()
                with open(out_file, "w", encoding="utf-8") as json_file:
                    json_file.write(response.text)
            except requests.exceptions.RequestException as e:
                raise Handle200Exception(Handle200Exception.ERROR, f"Errore durante il download del file: {e}") from e
            with open(out_file, "r", encoding="utf-8") as json_file:
                records = [json.loads(jl) for jl in json_file]
            return records

        def parse_arpae_data(arpae_data):
            # Process info data part (data array at index 0)
            df_infos = pd.DataFrame(
                [record["data"][0]["vars"] for record in arpae_data for _ in range(len(record["data"]) - 1)]
            )

            missing_info_codes = [
                info_code for info_code in list(self.info_dict_code2name.keys()) if info_code not in df_infos.columns
            ]
            present_info_codes = [
                info_code for info_code in list(self.info_dict_code2name.keys()) if info_code not in missing_info_codes
            ]

            df_infos = df_infos[[info_code for info_code in present_info_codes]]

            for info_code in present_info_codes:
                df_infos[info_code] = df_infos[info_code].apply(
                    lambda value: value["v"]
                    if value is not None and type(value) is dict and "v" in value
                    else value
                    if value is not None
                    else None
                )

            # Process variables data part (data array from index 1 to end)
            var_default_codes = ["timerange", "level"]
            df_vars = pd.DataFrame(
                [
                    v["vars"] | {v_default: v[v_default] for v_default in var_default_codes}
                    for record in arpae_data
                    for v in record["data"][1:]
                ]
            )

            missing_var_codes = [
                var_code for var_code in list(self.var_dict_code2name.keys()) if var_code not in df_vars.columns
            ]
            present_var_codes = [
                var_code for var_code in list(self.var_dict_code2name.keys()) if var_code not in missing_var_codes
            ]

            df_vars = df_vars[[var_code for var_code in present_var_codes] + var_default_codes]

            for var_code in present_var_codes:
                df_vars[var_code] = df_vars[var_code].apply(
                    lambda value: value["v"]
                    if value is not None and type(value) is dict and "v" in value
                    else value
                    if value is not None
                    else None
                )

            # Concat
            df = pd.concat((df_infos, df_vars), axis=1)
            # Setting datetime
            df["date_time"] = df.apply(
                lambda record: datetime.datetime(
                    record[self.info_dict_name2code["year"]],
                    record[self.info_dict_name2code["month"]],
                    record[self.info_dict_name2code["day"]],
                    record[self.info_dict_name2code["hour"]],
                    record[self.info_dict_name2code["minute"]],
                    record[self.info_dict_name2code["second"]],
                ),
                axis=1,
            )
            df = df.drop(
                [
                    self.info_dict_name2code["year"],
                    self.info_dict_name2code["month"],
                    self.info_dict_name2code["day"],
                    self.info_dict_name2code["hour"],
                    self.info_dict_name2code["minute"],
                    self.info_dict_name2code["second"],
                ],
                axis=1,
            )
            # Create geometry
            df["geometry"] = df.apply(
                lambda record: Point(
                    record[self.info_dict_name2code["longitude"]], record[self.info_dict_name2code["latitude"]]
                ),
                axis=1,
            )
            # To GeoDataFrame
            gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
            return gdf

        arpae_meteo_data = request_arpae_data()
        arpae_meteo_gdf = parse_arpae_data(arpae_meteo_data)

        # Filter by variable B13215 (river_level)
        arpae_meteo_gdf = arpae_meteo_gdf[arpae_meteo_gdf[self.var_dict_name2code["river_level"]].notnull()]
        arpae_meteo_gdf = arpae_meteo_gdf[
            arpae_meteo_gdf["timerange"].apply(lambda trv: trv == [254, 0, 0])
        ]  # INFO: timerange code [254,0,0] means instantaneous data related to measure timestamp
        # Calculate Q from RiverLevel
        arpae_meteo_gdf["Q"] = arpae_meteo_gdf[self.var_dict_name2code["river_level"]].apply(
            lambda rl: self.portata_lineare(rl)
        )

        return arpae_meteo_gdf

    def filter_data(self, data, time_start, time_end, stations):
        if time_start is not None:
            data = data[data["date_time"] >= time_start]
        if time_end is not None:
            data = data[data["date_time"] <= time_end]
        if stations is not None:
            data = data[data[self.info_dict_name2code["station_name"]].isin(stations)]
        return data

    def portata_lineare(self, h_input):
        # INFO: Author: Paolo Mazzoli (paolo.mazzoli@gecosistema.com)
        # Tabella dati
        H = [
            0,
            0.03,
            0.05,
            0.06,
            0.09,
            0.1,
            0.15,
            0.2,
            0.25,
            0.3,
            0.35,
            0.4,
            0.45,
            0.5,
            0.55,
            0.6,
            0.65,
            0.7,
            0.75,
            0.8,
            0.85,
            0.9,
            0.95,
            1,
            1.05,
            1.1,
            1.15,
            1.2,
            1.25,
            1.3,
            1.31,
            1.36,
            1.44,
            1.52,
            1.6,
            1.66,
            1.68,
            1.73,
            1.85,
            1.95,
            2.03,
            2.1,
            2.15,
            2.2,
            2.25,
            2.3,
            2.35,
            2.4,
            2.45,
            2.5,
            2.55,
            2.6,
            2.65,
            2.7,
            2.75,
            2.78,
        ]
        Q = [
            0,
            0.49,
            1.41,
            1.72,
            3.59,
            4.38,
            9.85,
            16,
            22.8,
            30.1,
            37.7,
            45.6,
            53.7,
            61.9,
            70.3,
            79,
            87.8,
            96.8,
            106,
            115,
            125,
            134,
            144,
            153,
            163,
            172,
            182,
            192,
            202,
            213,
            215,
            239,
            258,
            277,
            296,
            312,
            326,
            341,
            363,
            392,
            417,
            437.3,
            453.3,
            470.0,
            487.4,
            505.8,
            525.1,
            545.4,
            566.7,
            589.2,
            738.8,
            637.8,
            664.0,
            691.6,
            720.7,
            739,
        ]
        # Se h_input è inferiore al minimo, restituisci la portata minima
        if h_input <= H[0]:
            return Q[0]
        # Se h_input è superiore al massimo, restituisci la portata massima
        if h_input >= H[-1]:
            return Q[-1]
        # Ricerca dell'intervallo per interpolazione lineare
        for i in range(len(H) - 1):
            if H[i] <= h_input <= H[i + 1]:
                h1, h2 = H[i], H[i + 1]
                q1, q2 = Q[i], Q[i + 1]
                # Interpolazione lineare
                q_interp = q1 + (q2 - q1) * (h_input - h1) / (h2 - h1)
                return q_interp
        return None  # fallback di sicurezza (non dovrebbe mai accadere)

    def create_var_stations_geojson(self, arpae_gdf):
        gdf_gr_st = arpae_gdf.groupby(self.info_dict_name2code["station_name"])
        features = []
        for _, (station, station_gdf) in enumerate(gdf_gr_st):
            feature = {
                "id": str(utils.string_to_int_id(station))
                if station not in self.station_ids
                else self.station_ids[station],  # INFO: Base62 conversion (biunivocal relation)
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [
                        station_gdf[self.info_dict_name2code["longitude"]].iloc[0],
                        station_gdf[self.info_dict_name2code["latitude"]].iloc[0],
                    ],
                },
                "properties": {
                    # INFO: Nome chiavi scelto
                    "EHYPE": 9780124,
                    "Factor": 1.0,
                    "stat_name": station,
                    "Q": station_gdf["Q"].to_list(),
                    "time": station_gdf["date_time"].apply(lambda dt: dt.isoformat()).to_list(),
                },
            }
            features.append(feature)
        station_geojson = {
            "type": "FeatureCollection",
            "features": features,
            "metadata": {
                "field": [{"@name": "Q", "@alias": "river_discharge", "@unit": "m**3/s", "@type": "discharge"}]
            },
            "crs": {
                "type": "name",
                "properties": {
                    "name": "urn:ogc:def:crs:OGC:1.3:CRS84"  # REF: https://gist.github.com/sgillies/1233327 lines 256:271
                },
            },
        }
        return station_geojson

    def update_collection(self, arpae_gdf, station_geojson):
        def update_s3_data(station_geojson):
            s3 = s3fs.S3FileSystem()
            try:
                with s3.open(self.collection_uri, "r") as f:
                    existing_geojson = json.loads(f.read())
                existing_stations = [feature["id"] for feature in existing_geojson["features"]]
                for feature in station_geojson["features"]:
                    if feature["id"] not in existing_stations:
                        existing_geojson["features"].append(feature)
                    else:
                        existing_feature = [f for f in existing_geojson["features"] if f["id"] == feature["id"]][0]
                        union_Q = existing_feature["properties"]["Q"] + feature["properties"]["Q"]
                        union_time = existing_feature["properties"]["time"] + feature["properties"]["time"]
                        unique_measures = (
                            pd.DataFrame({"Q": union_Q, "time": union_time})
                            .drop_duplicates(subset=["time"], keep="last")
                            .sort_values(by="time")
                        )
                        existing_feature["properties"]["Q"] = unique_measures["Q"].to_list()
                        existing_feature["properties"]["time"] = unique_measures["time"].to_list()
                with s3.open(self.collection_uri, "w") as f:
                    json.dump(existing_geojson, f)

            except FileNotFoundError:
                with s3.open(self.collection_uri, "w") as f:
                    json.dump(station_geojson, f)

        def update_pygeoapi_config(arpae_gdf):
            min_x, max_x = (
                arpae_gdf[self.info_dict_name2code["longitude"]].values.min().item(),
                arpae_gdf[self.info_dict_name2code["longitude"]].values.max().item(),
            )
            min_y, max_y = (
                arpae_gdf[self.info_dict_name2code["latitude"]].values.min().item(),
                arpae_gdf[self.info_dict_name2code["latitude"]].values.max().item(),
            )
            min_dt, max_dt = arpae_gdf["date_time"].min().isoformat(), arpae_gdf["date_time"].max().isoformat()

            lock = FileLock(f"{self.config_file}.lock", thread_local=False)
            with lock:
                config = utils.read_config(self.config_file)
                collection_exists = self.collection_id in config["resources"]
                collection_definition = {
                    "type": "collection",
                    "title": self.collection_id,
                    "description": "River discharge for EmiliaRomagna Stations",
                    "keywords": ["river-discharge", "arpae", "emilia-romagna", "italy"],
                    "extents": {},
                    "providers": [
                        {
                            "type": "feature",
                            "name": "S3GeoJSONProvider.S3GeoJSONProvider",
                            "data": self.collection_uri,
                            "id_field": "id",
                        }
                    ],
                }
                if collection_exists:
                    curr_min_x, curr_min_y, curr_max_x, curr_max_y = config["resources"][self.collection_id]["extents"][
                        "spatial"
                    ]["bbox"]
                    curr_min_dt, curr_max_dt = (
                        config["resources"][self.collection_id]["extents"]["temporal"]["begin"],
                        config["resources"][self.collection_id]["extents"]["temporal"]["end"],
                    )
                    collection_definition["extents"] = {
                        "spatial": {
                            "bbox": [
                                min(min_x, curr_min_x),
                                min(min_y, curr_min_y),
                                max(max_x, curr_max_x),
                                max(max_y, curr_max_y),
                            ],
                            "crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
                        },
                        "temporal": {"begin": min(min_dt, curr_min_dt), "end": max(max_dt, curr_max_dt)},
                    }
                else:
                    collection_definition["extents"] = {
                        "spatial": {
                            "bbox": [min_x, min_y, max_x, max_y],
                            "crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
                        },
                        "temporal": {"begin": min_dt, "end": max_dt},
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

        update_s3_data(station_geojson)
        update_pygeoapi_config(arpae_gdf)

    def execute(self, data):
        mimetype = "application/json"

        outputs = {}
        try:
            # Validate request
            time_start, time_end, stations = self.validate_parameters(data)

            # Get data from ARPAE
            arpae_gdf = self.get_arpae_data()

            # Filter data by variables, time and stations
            arpae_gdf = self.filter_data(arpae_gdf, time_start, time_end, stations)

            # Update S3 geojson collection
            station_geojson = self.create_var_stations_geojson(arpae_gdf)
            self.update_collection(arpae_gdf, station_geojson)

            # Return OK + geojson added data
            outputs = {"status": "OK", "ingested_data": station_geojson}

        except Handle200Exception as err:
            outputs = {"status": err.status, "message": str(err)}
        except Exception as err:
            raise ProcessorExecuteError(str(err)) from err

        return mimetype, outputs

    def __repr__(self):
        return f"<RerRiverDischargeArpaeProcessor> {self.name}"
