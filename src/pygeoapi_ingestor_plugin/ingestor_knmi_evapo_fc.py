import datetime
import logging
import os
import subprocess

import dask
import geopandas as gpd
import numpy as np
import pandas as pd
import rioxarray
import s3fs
import xarray as xr
import yaml
from dotenv import load_dotenv, find_dotenv
from filelock import FileLock
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
from pyproj import CRS
from sqlalchemy import create_engine

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


class IngestorKNMIobsforecastProcessProcessor(BaseProcessor):
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
        self.title = 'knmi_evapo_obs_forecast'
        self.otc_key = os.environ.get(key='FSSPEC_S3_KEY')
        self.otc_secret = os.environ.get(key='FSSPEC_S3_SECRET')
        self.otc_endpoint = os.environ.get(key='FSSPEC_S3_ENDPOINT_URL')
        self.db_user = os.environ.get(key='DB_USER', default="postgres")
        self.db_password = os.environ.get(key='DB_PASSWORD',default= "password")
        self.db_host = os.environ.get(key='DB_HOST', default="localhost")
        self.db_port = int(os.environ.get(key='DB_PORT', default="5432"))
        self.db_database = os.environ.get(key='DB_DATABASE', default="postgres")
        self.table_name = 'knmi_obs_forecast'
        self.current_year = datetime.datetime.now().year
        self.alternate_root = None
        self.obs = None
        self.forecast = None
        self.rio = rioxarray
        self.dask = dask.typing

        self.knmi_homo = None
        self.smhi_homo = None
        self.obs_forcast = None
        self.gdf = None

    def read_config(self):
        with open(self.config_file, 'r') as file:
            LOGGER.debug("read config")
            return (yaml.safe_load(file))

    def write_config(self, new_config):
        with  open(self.config_file, 'w') as outfile:
            yaml.dump(new_config, outfile, default_flow_style=False)
        LOGGER.debug("updated config")

    def update_zarr_config(self):
        da = self.obs_forcast
        min_x = float(da.lon.values.min())
        max_x = float(da.lon.values.max())
        min_y = float(da.lat.values.min())
        max_y = float(da.lat.values.max())

        # THIS MUST BE THE SAME IN ALL PROCESSES UPDATING THE SERV CONFIG
        lock = FileLock(f"{self.config_file}.lock")

        with lock:

            config= self.read_config()
            config['resources'][f'{self.title}_{self.current_year}'] = {
                'type': 'collection',
                'title': f'{self.title}_{self.current_year}',
                'description': f'{self.title} of {self.current_year}',
                'keywords': ['country'],
                'extents': {
                    'spatial': {
                        'bbox': [min_x, min_y, max_x, max_y],
                        'crs': 'http://www.opengis.net/def/crs/EPSG/0/4326'
                    },
                },
                'providers': [
                    {
                        'type': 'edr',
                        'name': 'xarray-edr',
                        'data': self.fc_zarr_out,
                        'x_field': 'lon',
                        'y_field': 'lat',
                        'time_field': 'time',
                        'format': {'name': 'zarr', 'mimetype': 'application/zip'},
                        #FIXME: check s3->anon: True should be working; maybe s3 vs http api
                        'options': {
                            's3': {
                                    'anon': False,
                                    'alternate_root': self.alternate_root,
                                    'endpoint_url': self.otc_endpoint,
                                    'requester_pays': False
                                }
                        }
                    }
                ]
            }

            self.write_config(config)

    def update_db_config(self):
        min_x, min_y, max_x, max_y = [float(val) for val in self.gdf.total_bounds]
        # THIS MUST BE THE SAME IN ALL PROCESSES UPDATING THE SERV CONFIG
        lock = FileLock(f"{self.config_file}.lock")

        with lock:

            config= self.read_config()
            config['resources'][f'{self.title}'] = {
                'type': 'collection',
                'title': f'{self.title}',
                'description': f'pot precip deficit obs',
                'keywords': ['country'],
                'extents': {
                    'spatial': {
                        'bbox': [min_x, min_y, max_x, max_y],
                        'crs': 'http://www.opengis.net/def/crs/EPSG/0/4326'
                    },
                },
                'providers':
                    [{
                        'type': 'feature',
                        'name': 'PostgreSQL',
                        'data': {
                            'host': self.db_host,
                            'port': self.db_port,
                            'dbname': self.db_database,
                            'user': self.db_user,
                            'password': self.db_password,
                            'search_path': ['public']
                        },
                        'id_field': 'index',
                        'table': f'{self.table_name}',
                        'geom_field': 'geometry'
                    }]
            }

            self.write_config(config)

    def homogenize_ds(self):
        kkk = xr.open_zarr(self.obs).rio.write_crs(
            28992)
        sss = xr.open_zarr(self.forecast).rio.write_crs(
            4326)

        kkk = kkk.rio.reproject(CRS.from_epsg(4326))
        kkk = kkk.rename({'x': 'lon', 'y': 'lat'})
        sss = sss.interp(lon=kkk.lon, lat=kkk.lat, method="nearest")

        return kkk, sss

    def calc_p_def(self, pet0, pet1):
        p_def = np.where(np.logical_or(np.isnan(pet0), np.isnan(pet1)), np.nan,
                         np.where((pet0 + pet1 > 0), pet0 + pet1, 0))
        return p_def

    def calc_p_def_from_obs(self):
        pet0 = self.knmi_homo["p_def"].isel(time=-1).values
        start_time = self.knmi_homo["time"].isel(time=-1).values + np.timedelta64(1, 'D')
        forecast_period = self.smhi_homo['delta_precip_def'].sel(time=slice(start_time, None)).values

        res = []
        res.append(self.calc_p_def(pet0, forecast_period[:, 0, :, :]))
        for i in range(1, forecast_period.shape[1]):
            res.append(self.calc_p_def(res[-1], forecast_period[:, i, :, :]))
        result_array = np.stack(res, axis=1)

        final_xr = xr.DataArray(
            result_array,
            dims=["quantile", "time", "lat", "lon"],
            coords={
                "quantile": self.smhi_homo["quantile"],
                "time": self.smhi_homo["time"].isel(time=slice(len(self.smhi_homo["time"]) - result_array.shape[1], None)),
                "lat": self.knmi_homo["lat"],
                "lon": self.knmi_homo["lon"]
            },
            name="p_def"
        )
        final_xr = final_xr.to_dataset(name="p_def")

        return final_xr

    def prepare_tab(self, date):
        gdf = gpd.read_file(f'/tmp/knmi/fc_obs/{date}.geojson').set_crs(epsg=4326, inplace=True).drop(columns='ID')
        gdf['date'] = datetime.datetime.strptime(date, '%Y-%m-%d')
        return gdf

    def create_contours_df(self, final_xr):
        export_path = '/tmp/knmi/fc_obs'
        os.makedirs(export_path, exist_ok=True)
        for t in final_xr['time'].values:
            export_file = os.path.join(export_path, f'{t.astype(str)[:10]}.nc')
            final_xr['p_def'].sel(time=t, quantile=0.5).to_netcdf(export_file)

        date_strings = [f.strip(".nc") for f in os.listdir(export_path) if f.endswith('.nc')]
        fixed_lvls = [50, 100, 125, 150, 175, 200]
        for date_string in date_strings:
            command_tif = f"gdal_translate -a_ullr {float(final_xr.lon.values.min())} {float(final_xr.lat.values.max())} {float(final_xr.lon.values.max())} {float(final_xr.lat.values.min())} -a_srs EPSG:4326 {export_path}/{date_string}.nc {export_path}/{date_string}.tif"
            subprocess.run(command_tif, shell=True, check=True)
            command_geojson = f"""gdal_contour -p -amin lvlmin -amax lvlmax -fl {' '.join(str(lvl) for lvl in fixed_lvls)} {export_path}/{date_string}.tif {export_path}/{date_string}.geojson"""
            subprocess.run(command_geojson, shell=True, check=True)

        gdf_arr = [self.prepare_tab(date_string) for date_string in date_strings]
        gdf = pd.concat(gdf_arr, ignore_index=True)

        return gdf

    def execute(self, data):
        mimetype = 'application/json'
        self.fc_zarr_out = data.get('zarr_out')
        self.obs = data.get('obs')
        self.forecast = data.get('forecast')
        self.token = data.get('token')
        self.alternate_root = self.fc_zarr_out.split("s3://")[1] if self.fc_zarr_out is not None else "bibi"

        LOGGER.debug(f"checking process inputs")
        if self.fc_zarr_out is None or not self.fc_zarr_out.startswith('s3://'):
            raise ProcessorExecuteError('Cannot process without a zarr out path')
        if self.obs is None:
            raise ProcessorExecuteError('Cannot process without observations')
        if self.forecast is None:
            raise ProcessorExecuteError('Cannot process without forecasts')
        if self.token is None:
            raise ProcessorExecuteError('Identify yourself with valid token!')
        if self.token != os.getenv("INT_API_TOKEN", "token"):
            #FIXME passender error?
            LOGGER.error("WRONG INTERNAL API TOKEN")
            raise ProcessorExecuteError('ACCESS DENIED wrong token')

        LOGGER.debug(f"checking online resources")
        s3 = s3fs.S3FileSystem()
        if self.obs and self.obs.startswith('s3://'):
            if not s3.exists(self.obs):
                raise ProcessorExecuteError(f"""ressource '{self.obs}' does not exist""")
        if self.forecast and self.forecast.startswith('s3://'):
            if not s3.exists(self.forecast):
                raise ProcessorExecuteError(f"""ressource '{self.forecast}' does not exist""")

        store = s3fs.S3Map(root=self.fc_zarr_out, s3=s3, check=False)
        LOGGER.info(f'homogenizing ds')
        self.knmi_homo, self.smhi_homo = self.homogenize_ds()
        LOGGER.info(f'calculating precip deficit')
        self.obs_forcast = self.calc_p_def_from_obs()
        p_def_20 = self.obs_forcast['p_def'].sel(quantile=0.2).values
        p_def_50 = self.obs_forcast['p_def'].sel(quantile=0.5).values
        p_def_80 = self.obs_forcast['p_def'].sel(quantile=0.8).values

        final_xr = xr.DataArray(
            p_def_20,
            dims=["time", "lat", "lon"],
            coords={
                "time": self.obs_forcast["time"],
                "lat": self.obs_forcast["lat"],
                "lon": self.obs_forcast["lon"]
            },
            name="p_def_q20"
        )

        final_xr = final_xr.to_dataset(name="p_def_q20")
        final_xr['p_def_q50'] = (('time', 'lat', 'lon'), p_def_50)
        final_xr['p_def_q80'] = (('time', 'lat', 'lon'), p_def_80)

        LOGGER.info(f"upload to '{self.fc_zarr_out}'")
        final_xr.to_zarr(store, consolidated=True, mode='w')




        engine = create_engine(
            f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_database}")
        LOGGER.info(f'creating contours')
        self.gdf = self.create_contours_df(self.obs_forcast)
        nl_landmasses_clip = gpd.read_file("/pygeoapi/secondary_process_data/NL_landmasses_4326_50thdegreebuffer.geojson")
        forecast_gdf = self.gdf.clip(nl_landmasses_clip)
        LOGGER.info(f'put vector data in db')
        forecast_gdf.to_postgis(self.table_name, engine, if_exists='replace', index=True)
        engine.dispose()


        self.update_zarr_config()
        self.update_db_config()

        outputs = {
            'id': 'knmi_evapo_ingestor_obs_forecast',
            'value': self.fc_zarr_out
        }

        return mimetype, outputs

    def __repr__(self):
        return f'<IngestorKNMIobsforecastProcessProcessor> {self.name}'
