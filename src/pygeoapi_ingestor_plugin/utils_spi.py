import os
import math
import json
import shutil
import logging
import datetime
from dateutil import relativedelta
import tempfile
from filelock import FileLock

import numpy as np
import pandas as pd
import xarray as xr
import pygrib
import geopandas as gpd

from rasterio.enums import Resampling

import scipy.stats as stats
from scipy.special import gammainc, gamma

import s3fs

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

import pygeoapi_ingestor_plugin.utils_s3 as s3_utils
import pygeoapi_ingestor_plugin.utils as utils

LOGGER = logging.getLogger(__name__)




_temp_dir = os.path.join(tempfile.gettempdir(), 'IngestorCDSSPIProcessProcessor')
if not os.path.exists(_temp_dir):
    os.makedirs(_temp_dir, exist_ok=True)

_config_file = os.environ.get(default='/pygeoapi/serv-config/local.config.yml', key='PYGEOAPI_SERV_CONFIG')

_reference_period = (datetime.datetime(1980, 1, 1), datetime.datetime(2010, 12, 31))  # REF: https://drought.emergency.copernicus.eu/data/factsheets/factsheet_spi.pdf

_living_lab_bbox = {
    'georgia': {
        'min_x': 44,  # W
        'min_y': 40,  # S
        'max_x': 48,  # E
        'max_y': 43   # N
    }
}
_living_lab_basin = {
    'georgia': 'data/georgia_basins.geojson'
}

_s3_bucket = os.environ.get("DEFAULT_BUCKET")
remote_path = os.environ.get("DEFAULT_REMOTE_DIR")
_s3_living_lab_ref_data = {
    'georgia': f"s3://{_s3_bucket}/{remote_path}georgia/spi/reference_data/era5_land__total_precipitation__georgia__monthly__1950_2024.nc"
}
_s3_spi_collection_zarr_uris = {
    'georgia': {
        'historic': lambda date: f's3://{_s3_bucket}/{remote_path}georgia/spi/spi_historic_{date.strftime("%Y-%m")}.zarr',
        'forecast': lambda date: f's3://{_s3_bucket}/{remote_path}georgia/spi/spi_forecast_{date.strftime("%Y-%m")}.zarr'
    }
}
_s3_spi_collection_zonal_stats_uris = {
    'georgia': {
        'historic': lambda date: f's3://{_s3_bucket}/{remote_path}georgia/spi/spi_historic_{date.strftime("%Y-%m")}_zonal_stats.geojson',
        'forecast': lambda date: f's3://{_s3_bucket}/{remote_path}georgia/spi/spi_forecast_{date.strftime("%Y-%m")}_zonal_stats.geojson'
    }
}

_collection_pygeoapi_identifiers = {
    'georgia': {
        'historic': lambda date: f'georgia_spi_historic_{date.strftime("%Y-%m")}',
        'forecast': lambda date: f'georgia_spi_forecast_{date.strftime("%Y-%m")}'
    }
}


class Handle200Exception(Exception):

    OK = 'OK'
    SKIPPED = 'SKIPPED'
    PARTIAL = 'PARTIAL'

    def __init__(self, status, message):
        self.status = status
        self.message = message
        super().__init__(self.message)




def validate_parameters(data, data_type):
    """
    Validate request parameters
    """
    LOGGER.debug("era5 file path at: " + _s3_living_lab_ref_data['georgia'])
    token = data.get('token', None)
    living_lab = data.get('living_lab', None)
    period_of_interest = data.get('period_of_interest', None)
    spi_ts = data.get('spi_ts', None)
    out_format = data.get('out_format', None)
    cron_invocation = data.get('cron_invocation', False)

    if cron_invocation:
        if data_type == 'historic':
            period_of_interest = (datetime.datetime.now() - relativedelta.relativedelta(months=1)).strftime("%Y-%m") # INFO: previous month .. historic data for current month will never exists
        elif data_type == 'forecast':
            period_of_interest = datetime.datetime.now().strftime("%Y-%m")

    LOGGER.debug(f"period_of_interest: {period_of_interest}")

    if token is None:
        raise ProcessorExecuteError('You must provide an valid token')
    if token != os.getenv("INT_API_TOKEN", "token"):
        LOGGER.error(f"WRONG INTERNAL API TOKEN {token} ({type(token)}) != {os.getenv('INT_API_TOKEN', 'token')} ({type(os.getenv('INT_API_TOKEN', 'token'))})")
        raise ProcessorExecuteError('ACCESS DENIED: wrong token')

    if living_lab is None:
        raise ProcessorExecuteError('Cannot process without a living_lab valued')
    if type(living_lab) is not str:
        raise ProcessorExecuteError('living_lab must be a string')
    if living_lab not in _living_lab_bbox.keys():
        raise ProcessorExecuteError(f'living_lab must be one of {[f"{ll}" for ll in list(_living_lab_bbox.keys())]})')

    def validate_period_of_interest_historic(period_of_interest):
        if period_of_interest is None:
            raise ProcessorExecuteError('Cannot process without a period_of_interest valued')
        if type(period_of_interest) is not str:
            raise ProcessorExecuteError('period_of_interest must be a string')
        try:
            period_of_interest = datetime.datetime.strptime(period_of_interest, "%Y-%m")
            if period_of_interest.strftime("%Y-%m") >= datetime.datetime.now().strftime("%Y-%m"):
                raise Handle200Exception(Handle200Exception.SKIPPED, 'period_of_interest must be a date before current date month')
        except ValueError:
            raise ProcessorExecuteError('period_of_interest must be a valid datetime YYYY-MM string')
        period_of_interest = [
            period_of_interest.date(),
            period_of_interest.replace(day=utils.days_in_month(period_of_interest)).date()
        ]
        return period_of_interest

    def validate_period_of_interest_forecast(period_of_interest):
        diff_months = lambda date1, date2: (date2.year - date1.year) * 12 + (date2.month - date1.month)
        if period_of_interest is None:
            raise ProcessorExecuteError('Cannot process without a period_of_interest valued')
        if type(period_of_interest) is not str:
            raise ProcessorExecuteError('period_of_interest must be a string')
        try:
            period_of_interest = datetime.datetime.strptime(period_of_interest, "%Y-%m")

            # INFO: We are able to retrieve forecast data of past months
            # if period_of_interest.strftime("%Y-%m") < datetime.datetime.now().strftime("%Y-%m"):
            #     raise ProcessorExecuteError('period_of_interest must be a date after current date month')

            # INFO: If requested period of interest is in the current month or later it will be used last available init month for the forecast data
            if period_of_interest.strftime("%Y-%m") == datetime.datetime.now().strftime("%Y-%m") and datetime.datetime.now() <= datetime.datetime.now().replace(day=6, hour=12, minute=0, second=0):
                raise Handle200Exception(Handle200Exception.SKIPPED, 'period_of_interest in current month is available from day 6 at 12UTC')

            # INFO: If requested period is in future months, even if some forecast data are already available, we will not use them. We wait until the init month of the requested period is available
            if period_of_interest.strftime("%Y-%m") > datetime.datetime.now().strftime("%Y-%m"):
                raise Handle200Exception(Handle200Exception.SKIPPED, f'period_of_interest of {period_of_interest.strftime("%Y-%m")} will be available from day {period_of_interest.strftime("%Y-%m")}-06 at 12UTC')

            if diff_months(datetime.datetime.now(), period_of_interest) > 6:
                raise Handle200Exception(Handle200Exception.SKIPPED, 'period_of_interest must be within 6 months from current date')
        except ValueError:
            raise ProcessorExecuteError('period_of_interest must be a valid datetime YYYY-MM string')
        period_of_interest = [
            period_of_interest.date(),
            (period_of_interest + datetime.timedelta(hours=5160)).replace(day=1, hour=0).date() # INFO: If we want all the forecast data of the declared month: 6 month # REF: https://cds.climate.copernicus.eu/datasets/seasonal-original-single-levels?tab=download#leadtime_hour
            # period_of_interest.replace(day=utils.days_in_month(period_of_interest)).date() # INFO: If we want data only of the declared month into the period of interest instaed aff all 5160 avaliable forecast hour # REF: https://cds.climate.copernicus.eu/datasets/seasonal-original-single-levels?tab=download#leadtime_hour
        ]
        return period_of_interest

    if data_type == 'historic':
        period_of_interest = validate_period_of_interest_historic(period_of_interest)
    elif data_type == 'forecast':
        period_of_interest = validate_period_of_interest_forecast(period_of_interest)
    else:
        raise ProcessorExecuteError('data_type must be one of ["forecast", "historic"]')

    if spi_ts is None:
        raise ProcessorExecuteError('Cannot process without a spi_ts valued')
    if type(spi_ts) is not int:
        raise ProcessorExecuteError('spi_ts must be an integer')
    if spi_ts not in [1]: # TODO: timescales [3,6,12,24,48] to be implemented
        raise ProcessorExecuteError('spi_ts must be 1. Other values are not supported yet')

    if out_format is not None:
        if type(out_format) is not str:
            raise ProcessorExecuteError('out_format must be a string or null')
        if out_format not in ['netcdf', 'json', 'dataframe', 'tif', 'zarr']:
            raise ProcessorExecuteError('out_format must be one of ["netcdf", "json", "dataframe", "tif", "zarr"]')

    def check_s3_path_exists(living_lab, period_of_interest, data_type):
        """Check if the S3 path already exists."""
        raise_200_exception = None        
        s3_zarr_collection_uri = _s3_spi_collection_zarr_uris[living_lab][data_type](date = period_of_interest[0])
        pygeoapi_zarr_collection_id = _collection_pygeoapi_identifiers[living_lab][data_type](date = period_of_interest[0])
        if s3_zarr_collection_uri.startswith('s3://'):
            s3 = s3fs.S3FileSystem()
            if s3.exists(s3_zarr_collection_uri):
                if pygeoapi_zarr_collection_id in utils.read_config(_config_file)['resources']:
                    raise_200_exception = {'status': Handle200Exception.OK, 'message': f"Path {s3_zarr_collection_uri} already exists in bucket and config"}
                else:
                    ds = xr.open_zarr(s3_zarr_collection_uri)
                    collection_params = {
                        's3_uri': s3_zarr_collection_uri,
                        'bbox': [ds.lon.values.min().item(), ds.lat.values.min().item(), ds.lon.values.max().item(), ds.lat.values.max().item()],
                        'time': {
                            'begin': ds.time.min().dt.date.item(),
                            'end': ds.time.max().dt.date.item()
                        },
                        'pygeoapi_id': pygeoapi_zarr_collection_id,
                        'providers': [
                            {
                                'type': 'edr',
                                'name': 'xarray-edr',
                                'data': s3_zarr_collection_uri,
                                'x_field': 'lon',
                                'y_field': 'lat',
                                'time_field': 'time',
                                'format': {
                                    'name': 'zarr',
                                    'mimetype': 'application/zip'
                                }
                            }
                        ]
                    }
                    update_config(living_lab, collection_params)
                    raise_200_exception = {'status': Handle200Exception.PARTIAL, 'message': f"Path {s3_zarr_collection_uri} already exists in bucket, updates config at '{_config_file}'"}
        
        if data_type == 'forecast' and _living_lab_basin.get(living_lab, None) is not None:
            s3_zonal_stats_collection_uri = _s3_spi_collection_zonal_stats_uris[living_lab][data_type](date = period_of_interest[0])
            pygeoapi_zonal_stats_collection_id = f"{pygeoapi_zarr_collection_id}_zonal_stats" 
            if s3_zonal_stats_collection_uri.startswith('s3://'):
                s3 = s3fs.S3FileSystem()
                if s3.exists(s3_zonal_stats_collection_uri):
                    if pygeoapi_zonal_stats_collection_id in utils.read_config(_config_file)['resources']:
                        raise_msg = f"Path {s3_zonal_stats_collection_uri} already exists in bucket and config"
                        raise_200_exception = {
                            'status': raise_200_exception['status'] if raise_200_exception.get('status', None) else Handle200Exception.OK,
                            'message': f"{raise_200_exception['message']}. {raise_msg}" if raise_200_exception.get('message', None) else raise_msg
                        }
                    else:
                        if s3.exists(s3_zarr_collection_uri):
                            ds = xr.open_zarr(s3_zarr_collection_uri)
                            zarr_collection_params = {
                                's3_uri': s3_zarr_collection_uri,
                                'bbox': [ds.lon.values.min().item(), ds.lat.values.min().item(), ds.lon.values.max().item(), ds.lat.values.max().item()],
                                'time': {
                                    'begin': ds.time.min().dt.date.item(),
                                    'end': ds.time.max().dt.date.item()
                                },
                                'pygeoapi_id': pygeoapi_zarr_collection_id,
                                'providers': [
                                    {
                                        'type': 'edr',
                                        'name': 'xarray-edr',
                                        'data': s3_zarr_collection_uri,
                                        'x_field': 'lon',
                                        'y_field': 'lat',
                                        'time_field': 'time',
                                        'format': {
                                            'name': 'zarr',
                                            'mimetype': 'application/zip'
                                        }
                                    }
                                ]
                            }
                            geojson_collection_params = zarr_collection_params.copy()
                            geojson_collection_params['s3_uri'] = s3_zonal_stats_collection_uri
                            geojson_collection_params['pygeoapi_id'] = f"{zarr_collection_params['pygeoapi_id']}_zonal_stats"
                            geojson_collection_params['providers'] = [
                                {
                                    'type': 'feature',
                                    'name': 'S3GeoJSONProvider.S3GeoJSONProvider',
                                    'data': s3_zonal_stats_collection_uri,
                                    'id_field': 'id'
                                }
                            ]
                            update_config(living_lab, geojson_collection_params)
                            
                            raise_msg = f"Path {s3_zonal_stats_collection_uri} already exists in bucket, updates config at '{_config_file}'"
                            raise_200_exception = {
                                'status': Handle200Exception.PARTIAL,
                                'message': f"{raise_200_exception['message']}. {raise_msg}" if raise_200_exception.get('message', None) else raise_msg
                            }
                else:
                    if s3.exists(s3_zarr_collection_uri):
                        ds = xr.open_zarr(s3_zarr_collection_uri)
                        zarr_collection_params = {
                                's3_uri': s3_zarr_collection_uri,
                                'bbox': [ds.lon.values.min().item(), ds.lat.values.min().item(), ds.lon.values.max().item(), ds.lat.values.max().item()],
                                'time': {
                                    'begin': ds.time.min().dt.date.item(),
                                    'end': ds.time.max().dt.date.item()
                                },
                                'pygeoapi_id': pygeoapi_zarr_collection_id,
                                'providers': [
                                    {
                                        'type': 'edr',
                                        'name': 'xarray-edr',
                                        'data': s3_zarr_collection_uri,
                                        'x_field': 'lon',
                                        'y_field': 'lat',
                                        'time_field': 'time',
                                        'format': {
                                            'name': 'zarr',
                                            'mimetype': 'application/zip'
                                        }
                                    }
                                ]
                            }
                        ds_zonal_stats = compute_zonal_stats(living_lab, ds)
                        geojson_collection_params = create_s3_zonal_stats_collection_data(living_lab, ds_zonal_stats, zarr_collection_params, data_type='forecast')
                        update_config(living_lab, geojson_collection_params)
                        raise_msg = f"Path {s3_zonal_stats_collection_uri} computed using {s3_zarr_collection_uri}, and uploaded to bucket, updates config at '{_config_file}'"
                        raise_200_exception = {
                            'status': Handle200Exception.PARTIAL,
                            'message': f"{raise_200_exception['message']}. {raise_msg}" if raise_200_exception.get('message', None) else raise_msg
                        }
                    # INFO: Else if the zonal stats collection exists on S3 but the SPI collection is not available, we have to redo the whole process for safety (Zonal is strongly dependent on SPI)
                    
        return raise_200_exception

    raise_200_exception = check_s3_path_exists(living_lab, period_of_interest, data_type)
    if raise_200_exception is not None:
        raise Handle200Exception(raise_200_exception['status'], raise_200_exception['message'])

    LOGGER.debug('parameters validated')
    return living_lab, period_of_interest, spi_ts, out_format



def format_params_for_poi_cds_query(period_of_interest):
    if len(period_of_interest) == 1:
        period_of_interest = [
            period_of_interest[0].date().replace(day=1),
            period_of_interest[0].date().replace(day=utils.days_in_month(period_of_interest[0]))
        ]
    else:
        period_of_interest = [
            period_of_interest[0].date().replace(day=1),
            period_of_interest[1].date().replace(day=utils.days_in_month(period_of_interest[1]))
        ]
    return period_of_interest



def grib2xr(grib_filename, grib_var_name, xr_var_name=None):
    grib_ds = pygrib.open(grib_filename)
    grib_ds_msgs = [msg for msg in list(grib_ds) if msg.name==grib_var_name]
    lat_range = grib_ds_msgs[0].data()[1][:,0]
    lon_range = grib_ds_msgs[0].data()[2][0,:]
    var_data = []
    times_range = []
    for i,msg in enumerate(grib_ds_msgs):
        values, _, _ = msg.data()
        data = np.stack(values)
        var_data.append(data)
        times_range.append(msg.validDate)
    var_dataset = np.stack(var_data)
    xr_var_name = grib_var_name.replace(' ','_').lower() if xr_var_name is None else xr_var_name
    xr_dataset = xr.Dataset(
        {
            xr_var_name: (["time", "lat", "lon"], var_dataset)
        },
        coords={
            "time": times_range,
            "lat": lat_range,
            "lon": lon_range
        }
    )
    return xr_dataset



def read_ref_cds_data(living_lab):
    """
    Read reference data from S3.
    Slice them in the bbox range and a default reference period.

    REF: https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land-monthly-means
    """
    fs = s3fs.S3FileSystem()

    with fs.open(_s3_living_lab_ref_data[living_lab]) as fileObj:
        cds_ref_data = xr.open_dataset(fileObj)

    cds_ref_data = cds_ref_data.sortby(['time', 'lat', 'lon'])
    cds_ref_data = cds_ref_data.sel(time = slice(*_reference_period))

    LOGGER.debug('reference data read')
    return cds_ref_data



def preprocess_ref_dataset(ref_dataset):
    ref_dataset = ref_dataset * ref_dataset['time'].dt.days_in_month
    ref_dataset = ref_dataset.assign_coords(
        lat=np.round(ref_dataset.lat.values, 6),
        lon=np.round(ref_dataset.lon.values, 6),
    )
    ref_dataset = ref_dataset.sortby(['time', 'lat', 'lon'])
    return ref_dataset



def compute_timeseries_spi(monthly_data, spi_ts, nt_return=1):
        """
        Compute SPI index for a time series of monthly data

        REF: https://drought.emergency.copernicus.eu/data/factsheets/factsheet_spi.pdf
        REF: https://mountainscholar.org/items/842b69e8-a465-4aeb-b7ec-021703baa6af [ page 18 to 24 ]
        """

        df = pd.DataFrame({'monthly_data': monthly_data})

        # Totalled data over t_scale rolling windows
        if spi_ts > 1:
            t_scaled_monthly_data = df.rolling(spi_ts).sum().monthly_data.iloc[spi_ts:]
        else:
            t_scaled_monthly_data = df.monthly_data

        # Gamma fitted params
        a, _, b = stats.gamma.fit(t_scaled_monthly_data, floc=0)

        # Cumulative probability distribution
        G = lambda x: stats.gamma.cdf(x, a=a, loc=0, scale=b)

        m = (t_scaled_monthly_data==0).sum()
        n = len(t_scaled_monthly_data)
        q = m / n # zero prob

        H = lambda x: q + (1-q) * G(x) # zero correction

        t = lambda Hx: math.sqrt(
            math.log(1 /
            (math.pow(Hx, 2) if 0<Hx<=0.5 else math.pow(1-Hx, 2))
        ))

        c0, c1, c2 = 2.515517, 0.802853, 0.010328
        d1, d2, d3 = 1.432788, 0.189269, 0.001308

        Hxs = t_scaled_monthly_data[-spi_ts:].apply(H)
        txs = Hxs.apply(t)

        Z = lambda Hx, tx: ( tx - ((c0 + c1*tx + c2*math.pow(tx,2)) / (1 + d1*tx + d2*math.pow(tx,2) + d3*math.pow(tx,3) )) ) * (-1 if 0<Hx<=0.5 else 1)

        spi_t_indexes = pd.DataFrame(zip(Hxs, txs), columns=['H','t']).apply(lambda x: Z(x.H, x.t), axis=1).to_list()

        return np.array(spi_t_indexes[-nt_return]) if nt_return==1 else np.array(spi_t_indexes[-nt_return:])



def create_s3_collection_data(living_lab, ds, data_type):
    collection_date = ds.time.min().dt.date.item()

    s3_zarr_collection_uri = _s3_spi_collection_zarr_uris[living_lab][data_type](date = collection_date)
    s3 = s3fs.S3FileSystem()
    s3_store = s3fs.S3Map(root=s3_zarr_collection_uri, s3=s3, check=False)

    min_x, max_x = ds.lon.values.min().item(), ds.lon.values.max().item()
    min_y, max_y = ds.lat.values.min().item(), ds.lat.values.max().item()
    min_dt, max_dt = ds.time.min().dt.date.item(), ds.time.max().dt.date.item()

    collection_params = {
        's3_uri': s3_zarr_collection_uri,
        'bbox': [min_x, min_y, max_x, max_y],
        'time': {
            'begin': min_dt,
            'end': max_dt
        },

        'pygeoapi_id': _collection_pygeoapi_identifiers[living_lab][data_type](date = collection_date),
        
        'providers': [
            {
                'type': 'edr',
                'name': 'xarray-edr',
                'data': s3_zarr_collection_uri,
                'x_field': 'lon',
                'y_field': 'lat',
                'time_field': 'time',
                'format': {
                    'name': 'zarr',
                    'mimetype': 'application/zip'
                }
            }
        ]
    }

    ds.to_zarr(store=s3_store, consolidated=True, mode='w')

    return collection_params



def update_config(living_lab, collection_params):
    data_src = collection_params['s3_uri']

    # THIS MUST BE THE SAME IN ALL PROCESSES UPDATING THE SERV CONFIG
    lock = FileLock(f"{_config_file}.lock", thread_local=False)
    with lock:
        config = utils.read_config(_config_file)

        collection_pygeoapi_identifier = collection_params['pygeoapi_id']

        LOGGER.info(f"resource identifier and title: '{collection_pygeoapi_identifier}'")
        dataset_definition = {
            'type': 'collection',
            'title': collection_pygeoapi_identifier,
            'description': f'SPI for {living_lab}', #TODO: maybe more info on bbox and time extents
            'keywords': ['country'],
            'extents': {
                'spatial': {
                    'bbox': collection_params['bbox'],
                    'crs': 'http://www.opengis.net/def/crs/OGC/1.3/CRS84'
                },
                'temporal': {
                    'begin': datetime.datetime.combine(collection_params['time']['begin'], datetime.time()),
                    'end': datetime.datetime.combine(collection_params['time']['end'], datetime.time())
                }
            },
            'providers': collection_params['providers']
        }

        config['resources'][collection_pygeoapi_identifier] = dataset_definition

        s3_is_anon_access = s3_utils.is_s3_anon_access()
        endpoint_url = os.environ.get(default="https://obs.eu-de.otc.t-systems.com", key='FSSPEC_S3_ENDPOINT_URL')
        alternate_root = data_src.split("s3://")[1]
        if s3_is_anon_access:
            config['resources'][collection_pygeoapi_identifier]['providers'][0]['options'] = {
                's3': {
                    'anon': True,
                    'requester_pays': False
                }
            }
        else:
            config['resources'][collection_pygeoapi_identifier]['providers'][0]['options'] = {
                's3': {
                    'anon': False,
                            'alternate_root': alternate_root,
                            'endpoint_url': endpoint_url,
                            'requester_pays': False
                        }
                    }

        LOGGER.info(f"dataset definition to add: '{dataset_definition}'")

        utils.write_config(config_path=_config_file, config_out=config)
        
        
def compute_zonal_stats(living_lab, spi_dataset):
    
    def get_ds_basin(ds, basin_geometry):
        ds_basin = ds.rio.clip([basin_geometry], ds.rio.crs, all_touched=True)
        return ds_basin
    
    def to_time_r_lat_lon_array(ds):
        return np.moveaxis(ds.transpose('time', 'lat', 'lon').to_array().values, 1,0)
    
    def to_times_array(ds):
        return ds.time.dt.strftime('%Y%m').values
    
    def spi_ensemble(ds_zone):
        ensemble_values = np.nanmedian(to_time_r_lat_lon_array(ds_zone), axis=(1,2,3))
        return ensemble_values
    
    def spi_min(ds_zone):
        min_values = np.nanmin(to_time_r_lat_lon_array(ds_zone), axis=(1,2,3))
        return min_values
    
    def spi_max(ds_zone):
        max_values = np.nanmax(to_time_r_lat_lon_array(ds_zone), axis=(1,2,3))
        return max_values
    
    def spi_perc(ds_zone, perc):
        perc_values = np.nanpercentile(to_time_r_lat_lon_array(ds_zone), perc, axis=(1,2,3))
        return perc_values
    
    def basin_zonal_stat(ds, basin_gdf, stat_name, stat_func, stat_func_kwargs={}):
        stats = basin_gdf.geometry.apply(lambda geom: stat_func(get_ds_basin(ds, geom), **stat_func_kwargs))
        stats = np.stack(stats.values)
        times = to_times_array(ds)
        stat_df = pd.DataFrame(stats, columns=[f'{stat_name}_{t}' for t in times])
        basin_gdf = pd.concat((basin_gdf, stat_df), axis=1)
        return basin_gdf
    
    zonal_ds_path = _living_lab_basin.get(living_lab, None)
    
    if zonal_ds_path is not None:
        basin_gdf = gpd.read_file(zonal_ds_path)
        spi_dataset = spi_dataset.rio.write_crs("EPSG:4326").rio.set_spatial_dims('lon', 'lat')
        
        basin_gdf = basin_zonal_stat(spi_dataset, basin_gdf, 'min', spi_min)
        basin_gdf = basin_zonal_stat(spi_dataset, basin_gdf, 'perc05', spi_perc, {'perc': 5})
        basin_gdf = basin_zonal_stat(spi_dataset, basin_gdf, 'perc10', spi_perc, {'perc': 10})
        basin_gdf = basin_zonal_stat(spi_dataset, basin_gdf, 'perc30', spi_perc, {'perc': 30})
        basin_gdf = basin_zonal_stat(spi_dataset, basin_gdf, 'ensemble', spi_ensemble)
        basin_gdf = basin_zonal_stat(spi_dataset, basin_gdf, 'perc70', spi_perc, {'perc': 70})
        basin_gdf = basin_zonal_stat(spi_dataset, basin_gdf, 'perc90', spi_perc, {'perc': 90})
        basin_gdf = basin_zonal_stat(spi_dataset, basin_gdf, 'perc95', spi_perc, {'perc': 95})
        basin_gdf = basin_zonal_stat(spi_dataset, basin_gdf, 'max', spi_max)
        
        return basin_gdf
    
    else: 
        return None 
    
    
def create_s3_zonal_stats_collection_data(living_lab, zonal_stat_gdf, spi_coverage_collection_params, data_type):
    spi_zonal_stats_collection_params = spi_coverage_collection_params.copy()
    
    s3_zonal_stats_collection_uri = _s3_spi_collection_zonal_stats_uris[living_lab][data_type](date = spi_coverage_collection_params['time']['begin'])
    
    spi_zonal_stats_collection_params['s3_uri'] = s3_zonal_stats_collection_uri
    spi_zonal_stats_collection_params['pygeoapi_id'] = f"{spi_coverage_collection_params['pygeoapi_id']}_zonal_stats"
    
    spi_zonal_stats_collection_params['providers'] = [
        {
            'type': 'feature',
            'name': 'S3GeoJSONProvider.S3GeoJSONProvider',
            'data': s3_zonal_stats_collection_uri,
            'id_field': 'id'
        }
    ]
    
    zonal_stat_geojson = zonal_stat_gdf.to_geo_dict()
    for feature in zonal_stat_geojson['features']:
        feature['id'] = feature['properties']['SUBID']
        del feature['properties']['SUBID']
    s3 = s3fs.S3FileSystem()
    with s3.open(s3_zonal_stats_collection_uri, 'w') as f:
        f.write(str(zonal_stat_geojson))
    
    return spi_zonal_stats_collection_params


def coverages_to_out_format(coverages, out_format):
    out_coverages = []
    for coverage in coverages:
        out_coverages.append(coverage_to_out_format(coverage, out_format))
    return out_coverages


def coverage_to_out_format(coverage_ds, out_format):
    """
    Convert SPI coverage in the requested output format
    """

    coverage_out = None

    if out_format == 'netcdf':
        coverage_out = str(coverage_ds.to_netcdf())
    if out_format == 'json':
        coverage_out = json.loads(coverage_ds.to_dataframe().reset_index().to_json(orient='records'))
    if out_format == 'dataframe':
        coverage_out = coverage_ds.to_dataframe().reset_index().to_csv(sep=';', index=False, header=True)
    if out_format == 'tif':
        coverage_tif_filepath = os.path.join(_temp_dir, 'spi_coverage.tif')
        coverage_ds = coverage_ds.sortby('lat', ascending=False)
        coverage_ds.rio.write_crs("EPSG:4326", inplace=True)
        coverage_ds.rio.set_spatial_dims(x_dim='lon', y_dim='lat', inplace=True)
        coverage_ds.rio.to_raster(
            raster_path = coverage_tif_filepath,
            driver = "COG",
            compress = "DEFLATE",
            dtype = "float32",
            tiled = True,
            blocksize = 256,
            overview_resampling = Resampling.average
        )
        with open(coverage_tif_filepath, "rb") as f:
            tif_bytes = f.read()
        coverage_out = str(tif_bytes)
    if out_format == 'zarr':
        coverage_zarr_filepath = os.path.join(_temp_dir, 'spi_coverage.zarr')
        coverage_zarr_zip_filepath = os.path.join(_temp_dir, 'spi_coverage_zarr')
        coverage_ds.to_zarr(coverage_zarr_filepath, mode="w")
        shutil.make_archive(coverage_zarr_zip_filepath, "zip", coverage_zarr_filepath)  # Compress to .zip to transmit as bytes
        with open(coverage_zarr_zip_filepath, "rb") as f:
            zarr_bytes = f.read()
        coverage_out = str(zarr_bytes)

    LOGGER.debug(f'SPI coverage converted in {out_format} format')
    return coverage_out