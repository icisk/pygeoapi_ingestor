import datetime
import logging
import os
import subprocess

import fsspec
import geopandas as gpd
import numpy as np
import pandas as pd
import requests
import s3fs
import xarray as xr
import yaml
from dotenv import load_dotenv, find_dotenv
from filelock import FileLock
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
from sqlalchemy import create_engine, inspect

LOGGER = logging.getLogger(__name__)

load_dotenv(find_dotenv())

PROCESS_METADATA = {
    'version': '0.2.0',
    'id': 'knmi_evapo_ingestor',
    'title': {
        'en': 'knmi_evapo',
    },
    'description': {
        'en': 'downloads temp and precip data calculates potential evapotranspiration and cumsums it updating the online ressource; adds isolines to db'},
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
        }
    },
    'example': {
        "inputs": {
            "zarr_out": "s3://example/target/bucket.zarr",
            "token": "ABC123XYZ666"
        }
    }
}

def prepare_tab_data(path):
    date = datetime.datetime.strptime(os.path.basename(path).split(".")[0].split("_")[1], '%Y%m%d')
    gdf = gpd.read_file(path, geometry='geometry')
    gdf = gdf.drop(columns='ID')
    gdf = gdf.set_crs(epsg=28992, inplace=True)
    gdf = gdf.to_crs(epsg=4326)
    gdf['date'] = date
    return gdf

def get_db_data(engine):
    cmd = 'SELECT * FROM public.knmi_obs'
    gdf = gpd.read_postgis(cmd, engine, geom_col='geometry')
    gdf = gdf.drop(columns='index')
    return gdf

def merge_db_tab_data(db, tab):
    merged = gpd.GeoDataFrame(pd.concat([db, tab], ignore_index=True), geometry='geometry')
    return merged


class IngestorKNMIProcessProcessor(BaseProcessor):
    """
    Ingestor Processor

    calculates precip deficit and saves it to zarr format and uploads it to s3 bucket

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
        self.config_file = os.environ.get(default='/pygeoapi/serv-config/local.config.yml', key='PYGEOAPI_SERV_CONFIG')
        self.title = 'knmi_evapo_obs'
        self.otc_key = os.environ.get(key='FSSPEC_S3_KEY')
        self.otc_secret = os.environ.get(key='FSSPEC_S3_SECRET')
        self.otc_endpoint = os.environ.get(key='FSSPEC_S3_ENDPOINT_URL')
        self.knmi_api_key = os.environ.get(key='KNMI_API_KEY')
        self.knmi_header = dict(Authorization=self.knmi_api_key)
        self.smhi_server = os.environ.get(key='SMHI_FTP_SERVER', default='ftp.smhi.se')
        self.smhi_user = os.environ.get(key='SMHI_FTP_USER', default='icisk')
        self.smhi_passwd = os.environ.get(key='SMHI_FTP_PASSWORD')
        self.smhi_fc_root = 'living_labs/netherlands/gridded_seasonal_forecast'
        self.db_user = os.environ.get(key='DB_USER', default="postgres")
        self.db_password = os.environ.get(key='DB_PASSWORD',default= "password")
        self.db_host = os.environ.get(key='DB_HOST', default="localhost")
        self.db_port = int(os.environ.get(key='DB_PORT', default="5432"))
        self.db_database = os.environ.get(key='DB_DATABASE', default="postgres")
        self.table_name = 'knmi_obs'
        self.db_data = None
        self.alternate_root = None
        self.obs_zarr_out = None
        self.current_year = 2024 #datetime.datetime.now().year
        self.current_month = datetime.datetime.now().month
        self.mode = 'init' # 'init' - first time  or 'append' - every other time
        self.save_path = '/tmp/knmi'
        os.makedirs(self.save_path, exist_ok=True)
        self.online_obs_data = None


    def read_config(self):
        with open(self.config_file, 'r') as file:
            LOGGER.debug("read config")
            return(yaml.safe_load(file))

    def write_config(self, new_config):
        with  open(self.config_file, 'w') as outfile:
            yaml.dump(new_config, outfile, default_flow_style=False)
        LOGGER.debug("updated config")

    def get_data_from_cloud(self, zarr_path):
        mapper = fsspec.get_mapper(zarr_path,
                       alternate_root=self.alternate_root,
                       endpoint_url = self.otc_endpoint,
                       key=self.otc_key,
                       secret=self.otc_secret,
                       )
        return xr.open_zarr(mapper)


    def update_zarr_config(self):
        da = self.get_data_from_cloud(self.obs_zarr_out)
        min_x = float(da.x.values.min())
        max_x = float(da.x.values.max())
        min_y = float(da.y.values.min())
        max_y = float(da.y.values.max())

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
                        'crs': 'http://www.opengis.net/def/crs/EPSG/0/28992'
                    },
                },
                'providers': [
                    {
                        'type': 'edr',
                        'name': 'xarray-edr',
                        'data': self.obs_zarr_out,
                        'x_field': 'x',
                        'y_field': 'y',
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
        min_x, min_y, max_x, max_y = [float(val) for val in self.db_data.total_bounds]
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


    def check_config_if_ds_is_collection(self):
        config = self.read_config()
        return self.title in config['resources']

    def format_t(self, y, m, d, t=1, format='%Y%m%d'):
        d_start = datetime.date(y, m, d)
        td = datetime.timedelta(days=t)
        d_end = d_start + td
        return (d_start.strftime(format), d_end.strftime(format))

    def download_date_data(self,
                           date,
                           header,
                           params=dict(maxKeys=10, sorting="desc"),
                           data_source=dict(precip={
                               "base_url": 'https://api.dataplatform.knmi.nl/open-data/v1/datasets/Rd1nrt/versions/2/files'
                           }, temp={
                               "base_url": 'https://api.dataplatform.knmi.nl/open-data/v1/datasets/Tg1/versions/5/files'
                           })):
        save_path = self.save_path
        y, m, d = date
        ds, de = self.format_t(y, m, d)
        data_source['temp']['filename'] = f'INTER_OPER_R___TG1_____L3__{ds}T000000_{de}T000000_0006.nc'
        data_source['precip']['filename'] = f'INTER_OPER_R___RD1NRT__L3__{ds}T080000_{de}T080000_0002.nc'

        for var in data_source:
            LOGGER.debug(var)
            source = data_source[var]
            # getting date ds url
            try:
                res_dwn_url = requests.get(os.path.join(source['base_url'], source['filename'], 'url'), headers=header)
                res_dwn_url.raise_for_status()
                data_dwn_url = res_dwn_url.json()
                dwn_url = data_dwn_url['temporaryDownloadUrl']
            except requests.exceptions.HTTPError as e:
                LOGGER.debug(f""" download url error '{str(e)}'; HTML-code '{res_dwn_url.status_code}'""")
                return None

            # getting data itself
            try:
                res_data = requests.get(dwn_url)
                res_data.raise_for_status()
                data = res_data.content
                with open(os.path.join(save_path, source['filename']), 'wb') as f:
                    f.write(data)
            except requests.exceptions.HTTPError as e:
                f""" download data error '{str(e)}'; HTML-code '{res_data.status_code}'"""
                return None
            except IOError as e:
                print(f"error writing file: {str(e)}")
                return None

            data_source[var]['output_path'] = os.path.join(save_path, source['filename'])

        return (data_source)

    def get_Re(self, date, csv='/pygeoapi/secondary_process_data/tab_Re.csv'):
        y, m, d = date
        data = pd.read_csv(csv)
        return float(data['value'][data['x'] == f'{d}-{m}'].values[0])

    def calc_PET(self, Ta, date):
        Re = self.get_Re(date)
        kc = 1
        lam = 2.501 - (0.002361 * Ta)
        rho = 1000
        Tadd = 5
        Tscale = 45

        PET_value = kc * (Re / (lam * rho)) * ((Ta + Tadd) / Tscale) * 1000

        PET = np.where((Ta + Tadd > 0), PET_value, 0)

        return PET

    def calc_p_def(self, pet0, pet1):
        p_def = np.where((pet0 + pet1 > 0), pet0 + pet1, 0)
        return p_def

    def calc_init_p_def(self, pet0):
        p_def = np.where((pet0 > 0), pet0 , 0)
        return p_def


    def execute(self, data):
        mimetype = 'application/json'

        self.obs_zarr_out = data.get('zarr_out')
        self.token = data.get('token')
        self.alternate_root = self.obs_zarr_out.split("s3://")[1] if self.obs_zarr_out is not None else "bibi"

        LOGGER.debug(f"checking process inputs")

        if self.obs_zarr_out is None or not self.obs_zarr_out.startswith('s3://') :
            raise ProcessorExecuteError('Cannot process without a zarr path')
        if self.token is None:
            raise ProcessorExecuteError('Identify yourself with valid token!')

        if self.token != os.getenv("INT_API_TOKEN", "token"):
            #FIXME passender error?
            LOGGER.error("WRONG INTERNAL API TOKEN")
            raise ProcessorExecuteError('ACCESS DENIED wrong token')

        LOGGER.debug(f"checking online resources")
        if self.obs_zarr_out and self.obs_zarr_out.startswith('s3://'):
            s3 = s3fs.S3FileSystem()
            if s3.exists(self.obs_zarr_out):
                self.online_obs_data = self.get_data_from_cloud(self.obs_zarr_out)
                if self.title in self.read_config()['resources']:
                    self.mode = 'append'
                    msg = f"Path {self.obs_zarr_out} already exists in bucket and config; appending to dataset"
                else:
                    #FIXME gescheiten exit finden
                    self.update_zarr_config()
                    self.mode = 'append'
                    msg = f"Path {self.obs_zarr_out} already exists updates config at '{self.config_file}'; appending to dataset"
                #return mimetype, {'id': 'creaf_historic_ingestor', 'value': msg}
            else:
                msg = f"No online ressource '{self.obs_zarr_out}' available; initially creating one"

        LOGGER.info(msg)



        store = s3fs.S3Map(root=self.obs_zarr_out, s3=s3, check=False)
        if self.mode == 'init':
            date = datetime.date(self.current_year, 4, 1)
            # date = datetime.date(self.current_year, 12, 1) #für test

        if self.mode == 'append':
            date = datetime.datetime.fromtimestamp(self.online_obs_data.time.values[-1].astype('datetime64[s]').astype(int))
            date = date + datetime.timedelta(days=1)
            # TODO: nach testphase hier wieder legitimieren auch oben
            # if date >= datetime.datetime(self.current_year, 10, 1):
            #     return mimetype, {'id': 'knmi_ingestor', 'value': 'time window for defit precip isch over'}


        y = date.year
        m = date.month
        d = date.day

        LOGGER.debug(f"download data from date: '{y}-{m}-{d}'")
        summary = self.download_date_data((y, m, d), header=self.knmi_header)
        if summary is None:
            return mimetype, {'id': 'knmi_ingestor', 'value': 'process_failed!'}

        LOGGER.debug(f"processing files")
        t = xr.open_dataset(summary['temp']['output_path'])
        p = xr.open_dataset(summary['precip']['output_path'])

        t_vals = t['prediction'].values
        pet_vals = self.calc_PET(t_vals, (y, m, d))
        p_vals = p['prediction'].values
        p_def = pet_vals - p_vals
        #p_def = self.calc_p_def(pet_vals, p_vals)
        res = xr.Dataset(
            data_vars=dict(p_def=(['time', 'y', 'x'], p_def)),
            coords=dict(
                time=t.time.data,
                y=t.y.data,
                x=t.x.data
            )
        )

        LOGGER.debug(f"upload ZARR: '{self.obs_zarr_out}' in '{self.mode}-mode'")
        date_string = datetime.date(y, m , d).strftime('%Y%m%d')

        if self.mode == 'init':
            new_p_def = self.calc_init_p_def(res.isel(time=0)['p_def'])
            LOGGER.debug(f"min: {np.min(new_p_def)}; max: {np.max(new_p_def)}")
            new_res = xr.Dataset(
                data_vars=dict(p_def=(['time', 'y', 'x'], np.expand_dims(new_p_def.data, axis=0))),
                coords=dict(
                    time=t.time.data,
                    y=t.y.data,
                    x=t.x.data
                )
            )
            new_res.to_zarr(store=store, consolidated=True, mode='w')
            new_res.to_netcdf(f"/tmp/evapo_{date_string}.nc")
        if self.mode == 'append':
            new_p_def = self.calc_p_def(self.online_obs_data.isel(time=-1)['p_def'], res.isel(time=0)['p_def'])
            new_res = xr.Dataset(
                data_vars=dict(p_def=(['time', 'y', 'x'], np.expand_dims(new_p_def.data, axis=0))),
                coords=dict(
                    time=t.time.data,
                    y=t.y.data,
                    x=t.x.data
                )
            )
            new_res.to_zarr(store=store, consolidated=True, append_dim='time', mode='a')
            new_res.to_netcdf(f"/tmp/evapo_{date_string}.nc")

        self.update_zarr_config()

        command_tif = f"gdal_translate -a_ullr {float(res.x.values.min())} {float(res.y.values.max())} {float(res.x.values.max())} {float(res.y.values.min())} -a_srs EPSG:28992 /tmp/evapo_{date_string}.nc /tmp/evapo_{date_string}.tif"
        LOGGER.debug(command_tif)
        try:
            subprocess.run(command_tif, shell=True, check=True)
            LOGGER.debug("gdal_transform completed successfully.")
        except subprocess.CalledProcessError as e:
            LOGGER.error(f"Error: {e}")
            raise ProcessorExecuteError(f"""could not create tif from nc '{e}'""")

        if self.mode == 'init':
            iso_min = float(res['p_def'].min().values)
            iso_max = float(res['p_def'].max().values)
        if self.mode == 'append':
            iso_min = float(new_res['p_def'].min().values)
            iso_max = float(new_res['p_def'].max().values)

        # iso_diff = iso_max - iso_min
        #
        # if iso_diff <= 1:
        #     contour_step = 0.05
        # if iso_diff <= 3 and iso_diff > 1:
        #     contour_step = 0.2
        # if iso_diff <= 15 and iso_diff > 3:
        #     contour_step = 1
        # if iso_diff <= 30 and iso_diff > 15:
        #     contour_step = 5
        # if iso_diff > 30:
        #     contour_step = 10

        fixed_lvls = [50, 100, 125, 150, 175, 200]

        # command_iso_line = f"gdal_contour -a lvl -i {contour_step} /tmp/evapo_{date_string}.tif /tmp/evapo_{date_string}.geojson"
        command_iso_poly = f"""gdal_contour -p -amin lvlmin -amax lvlmax -fl {' '.join(str(lvl) for lvl in fixed_lvls)} /tmp/evapo_{date_string}.tif /tmp/evapo_{date_string}.geojson"""
        command_iso = command_iso_poly
        LOGGER.debug(command_iso)
        try:
            subprocess.run(command_iso, shell=True, check=True)
            LOGGER.debug("gdal_contour completed successfully.")
        except subprocess.CalledProcessError as e:
            LOGGER.error(f"Error: {e}")
            raise ProcessorExecuteError(f"""could not create contours from geojson '{e}'""")


        engine = create_engine(
            f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_database}")
        inspector = inspect(engine)
        table_exists = f'{self.table_name}' in inspector.get_table_names()

        nl_landmasses_clip = gpd.read_file("/pygeoapi/secondary_process_data/NL_landmasses_4326_50thdegreebuffer.geojson")

        if table_exists:
            db = get_db_data(engine)
            tab = prepare_tab_data(f"/tmp/evapo_{date_string}.geojson")
            tab = tab.clip(nl_landmasses_clip)
            merged = merge_db_tab_data(db, tab)
            merged.to_postgis('knmi_obs', engine, if_exists='replace', index=True)
            #psqltab.to_postgis('knmi_obs', engine, if_exists='append', index=False) # TODO find a way to only append - maybe saves memory
            self.db_data = merged
            engine.dispose()
        else:
            tab = prepare_tab_data(f"/tmp/evapo_{date_string}.geojson")
            tab = tab.clip(nl_landmasses_clip)
            tab.to_postgis('knmi_obs', engine, if_exists='replace', index=True)
            self.db_data = tab
            engine.dispose()

        self.update_db_config()

        outputs = {
            'id': 'knmi_evapo_ingestor',
            'value': self.obs_zarr_out
        }

        return mimetype, outputs

    def __repr__(self):
        return f'<IngestorKNMIProcessProcessor> {self.name}'
