import os
import math
import json
import shutil
import logging
import datetime
import tempfile
from filelock import FileLock

import numpy as np
import pandas as pd
import xarray as xr
import pygrib

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
        'min_x': 44.9,  # W
        'min_y': 41.1,  # S
        'max_x': 46.8,  # E
        'max_y': 42.3   # N
    }
}

_s3_bucket = f's3://saferplaces.co/test/icisk/spi/'
_s3_living_lab_ref_data = {
    'georgia': os.path.join(_s3_bucket, 'reference_data', 'era5_land__total_precipitation__georgia__monthly__1950_2024.nc')
}
_s3_spi_collection_zarr_uris = {
    'georgia': {
        'historic': os.path.join(_s3_bucket, 'spi_data/historic/spi_historic_georgia_test_02.zarr'),
        'forecast': os.path.join(_s3_bucket, 'spi_data/forecast/spi_forecast_georgia_test_12.zarr')
    }
}
_collection_pygeoapi_identifiers = {
    'georgia': {
        'historic': 'georgia_spi_historic',
        'forecast': 'georgia_spi_forecast'
    }
}




def validate_parameters(data, data_type):
    """
    Validate request parameters
    """
    
    living_lab = data.get('living_lab', None)
    period_of_interest = data.get('period_of_interest', None)
    spi_ts = data.get('spi_ts', None)
    out_format = data.get('out_format', None)
    
    if living_lab is None:
        raise ProcessorExecuteError('Cannot process without a living_lab valued')
    if type(living_lab) is not str:
        raise ProcessorExecuteError('living_lab must be a string')
    if living_lab not in _living_lab_bbox.keys():
        raise ProcessorExecuteError(f'living_lab must be one of {[f"{ll}" for ll in list(_living_lab_bbox.keys())]})')
    
    def validate_period_of_interest_historic(period_of_interest):
        if period_of_interest is None:
            raise ProcessorExecuteError('Cannot process without a period_of_interest valued')
        if type(period_of_interest) not in [str, list]:
            raise ProcessorExecuteError('period_of_interest must be a string or a list')
        if type(period_of_interest) is str:
            try:
                period_of_interest = datetime.datetime.fromisoformat(period_of_interest)
                if period_of_interest.strftime("%Y-%m") >= datetime.datetime.now().strftime("%Y-%m"):
                    raise ProcessorExecuteError('period_of_interest must be a date before current date month')                
            except ValueError:
                raise ProcessorExecuteError('period_of_interest must be a valid datetime iso-format string')
            period_of_interest = [period_of_interest]
        elif type(period_of_interest) is list:
            if len(period_of_interest) != 2:
                raise ProcessorExecuteError('period_of_interest list must have 2 elements')
            for poi in period_of_interest:
                try:
                    poi = datetime.datetime.fromisoformat(poi)
                    if poi.strftime("%Y-%m") >= datetime.datetime.now().strftime("%Y-%m"):
                        raise ProcessorExecuteError('period_of_interest must be a date before current date month')                
                except ValueError:
                    raise ProcessorExecuteError('period_of_interest must be a valid datetime iso-format string')
            period_of_interest = [datetime.datetime.fromisoformat(poi) for poi in period_of_interest]
            if period_of_interest[0] >= period_of_interest[1]:
                raise ProcessorExecuteError('period_of_interest[0] must be less than period_of_interest[1]')
            if period_of_interest[0].strftime("%Y-%m") == period_of_interest[1].strftime("%Y-%m"):
                raise ProcessorExecuteError('period_of_interest[0] and period_of_interest[1] must be in different months')
        return period_of_interest
    
    def validate_period_of_interest_forecast(period_of_interest):
        diff_months = lambda date1, date2: (date2.year - date1.year) * 12 + (date2.month - date1.month)
        if period_of_interest is None:
            raise ProcessorExecuteError('Cannot process without a period_of_interest valued')
        if type(period_of_interest) not in [str, list]:
            raise ProcessorExecuteError('period_of_interest must be a string or a list')
        if type(period_of_interest) is str:
            try:
                period_of_interest = datetime.datetime.fromisoformat(period_of_interest)
                if period_of_interest.strftime("%Y-%m") < datetime.datetime.now().strftime("%Y-%m"):
                    raise ProcessorExecuteError('period_of_interest must be a date after current date month') 
                if diff_months(datetime.datetime.now(), period_of_interest) > 6:
                    raise ProcessorExecuteError('period_of_interest must be within 6 months from current date')              
            except ValueError:
                raise ProcessorExecuteError('period_of_interest must be a valid datetime iso-format string')
            period_of_interest = [period_of_interest]
        elif type(period_of_interest) is list:
            if len(period_of_interest) != 2:
                raise ProcessorExecuteError('period_of_interest list must have 2 elements')
            for poi in period_of_interest:
                try:
                    poi = datetime.datetime.fromisoformat(poi)
                    if poi.strftime("%Y-%m") < datetime.datetime.now().strftime("%Y-%m"):
                        raise ProcessorExecuteError('period_of_interest must be a date after current date month')               
                except ValueError:
                    raise ProcessorExecuteError('period_of_interest must be a valid datetime iso-format string')
            period_of_interest = [datetime.datetime.fromisoformat(poi) for poi in period_of_interest]
            if period_of_interest[0] >= period_of_interest[1]:
                raise ProcessorExecuteError('period_of_interest[0] must be less than period_of_interest[1]')
            if period_of_interest[0].strftime("%Y-%m") == period_of_interest[1].strftime("%Y-%m"):
                raise ProcessorExecuteError('period_of_interest[0] and period_of_interest[1] must be in different months')
            if diff_months(datetime.datetime.now(), period_of_interest[0]) > 6:
                raise ProcessorExecuteError('period_of_interest[0] must be within 6 months from current date')
            if diff_months(datetime.datetime.now(), period_of_interest[1]) > 6:
                raise ProcessorExecuteError('period_of_interest[1] must be within 6 months from current date')
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
    
    if out_format is None:
        out_format = 'netcdf'
    if type(out_format) is not str:
        raise ProcessorExecuteError('out_format must be a string or null')
    if out_format not in ['netcdf', 'json', 'dataframe', 'tif', 'zarr']:
        raise ProcessorExecuteError('out_format must be one of ["netcdf", "json", "dataframe", "tif", "zarr"]')
    
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
    
    cds_ref_data_filepath = s3_utils.s3_download(
        uri = _s3_living_lab_ref_data[living_lab],
        fileout = os.path.join(_temp_dir, os.path.basename(_s3_living_lab_ref_data[living_lab]))
    )
    
    cds_ref_data = xr.open_dataset(cds_ref_data_filepath) 
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

        # Distribuzione probabilità cumulata
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
    


def update_s3_collection_data(living_lab, ds, data_type):
    s3_spi_collection_zarr_uri = _s3_spi_collection_zarr_uris[living_lab][data_type]
    s3 = s3fs.S3FileSystem()
    s3_store = s3fs.S3Map(root=s3_spi_collection_zarr_uri, s3=s3, check=False)
    
    min_x, max_x = ds.lon.values.min().item(), ds.lon.values.max().item()
    min_y, max_y = ds.lat.values.min().item(), ds.lat.values.max().item()
    min_dt, max_dt = datetime.datetime.fromtimestamp(ds.time.values.min().item() / 1e9), datetime.datetime.fromtimestamp(ds.time.values.max().item() / 1e9)
    
    config = utils.read_config(_config_file)
    collection_pygeoapi_identifier = _collection_pygeoapi_identifiers[living_lab][data_type]
    
    existing_collection = config['resources'].get(collection_pygeoapi_identifier, None)
    if existing_collection:
        curr_min_x, curr_min_y, curr_max_x, curr_max_y = existing_collection['extents']['spatial']['bbox']
        curr_min_dt = existing_collection['extents']['temporal']['begin']
        curr_max_dt = existing_collection['extents']['temporal']['end']

        # INFO: We assume and handle only the case of dataset with more recent datetime with an eventual overlap between last(s) dt of existing dataset and first(s) dt of new dataset
        if min_dt <= curr_max_dt:
            # There is an overlap
            curr_months_delta = ((curr_max_dt.year - curr_min_dt.year) * 12 + (curr_max_dt.month - curr_min_dt.month)) + 1
            months_overlap = ((curr_max_dt.year - min_dt.year) * 12 + (curr_max_dt.month - min_dt.month)) + 1
            if months_overlap == len(ds.time):
                # New dataset is a complete overlap of the latest part of the existing dataset
                if 'spatial_ref' in ds:
                    ds = ds.drop_vars(['spatial_ref'])
                ds.to_zarr(store=s3_store, consolidated=True, mode='a', region={
                    'time': slice(curr_months_delta-months_overlap, curr_months_delta),
                    'lat': slice(0, len(ds.lat)),
                    'lon': slice(0, len(ds.lon))
                })
            else:
                # New dataset is a partial overlap of the latest part of the existing dataset
                ds_overlap = ds.isel(time=[t for t in range(months_overlap)])
                if 'spatial_ref' in ds_overlap:
                    ds_overlap = ds_overlap.drop_vars(['spatial_ref'])
                ds_overlap.to_zarr(store=s3_store, consolidated=True, mode='a', region={
                    'time': slice(curr_months_delta-months_overlap, curr_months_delta),
                    'lat': slice(0, len(ds_overlap.lat)),
                    'lon': slice(0, len(ds_overlap.lon))
                })
                ds_exceed = ds.isel(time=[t for t in range(months_overlap, len(ds.time))])
                ds_exceed.to_zarr(store=s3_store, consolidated=True, mode='a', append_dim='time')
        else:
            # No overlap
            ds.to_zarr(store=s3_store, consolidated=True, mode='a', append_dim='time')
            
        min_x = min(min_x, curr_min_x)
        min_y = min(min_y, curr_min_y)
        max_x = max(max_x, curr_max_x)
        max_y = max(max_y, curr_max_y)
        min_dt = min(min_dt, curr_min_dt)
        max_dt = max(max_dt, curr_max_dt)
    else:
        # Collection does not exist yet, we will create it so we write all data
        ds.to_zarr(store=s3_store, consolidated=True, mode='a')
    
    updated_collection_params = {
        'bbox': [min_x, min_y, max_x, max_y],
        'time': {
            'begin': min_dt,
            'end': max_dt
        }
    }
    return updated_collection_params



def update_config(living_lab, updated_collection_params, data_type):
    data_src = _s3_spi_collection_zarr_uris[living_lab][data_type]

    # THIS MUST BE THE SAME IN ALL PROCESSES UPDATING THE SERV CONFIG
    lock = FileLock(f"{_config_file}.lock", thread_local=False)
    with lock:
        config = utils.read_config(_config_file)

        collection_pygeoapi_identifier = _collection_pygeoapi_identifiers[living_lab][data_type]
        
        LOGGER.info(f"resource identifier and title: '{collection_pygeoapi_identifier}'")
        dataset_definition = {
            'type': 'collection',
            'title': collection_pygeoapi_identifier,
            'description': f'SPI for {living_lab}', #TODO: maybe more info on bbox and time extents
            'keywords': ['country'],
            'extents': {
                'spatial': {
                    'bbox': updated_collection_params['bbox'],
                    'crs': 'http://www.opengis.net/def/crs/OGC/1.3/CRS84'
                },
                'temporal': {
                    'begin': updated_collection_params['time']['begin'],
                    'end': updated_collection_params['time']['end']
                }
            },
            'providers': [
                {
                    'type': 'edr',
                    'name': 'xarray-edr',
                    'data': data_src,
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

        config['resources'][collection_pygeoapi_identifier] = dataset_definition

        s3_is_anon_access = os.environ.get('S3_ANON_ACCESS', 'True') == 'True'
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



def build_spi_s3_uris(living_lab, periods_of_interest, spi_ts, data_type):
    s3_uris = []
    for period_of_interest in periods_of_interest:
        s3_uris.append(build_spi_s3_uri(living_lab, period_of_interest, spi_ts, data_type))
    return s3_uris
    
def build_spi_s3_uri(living_lab, period_of_interest, spi_ts, data_type):
    spi_part = f'spi-{spi_ts}'
    time_part = f'{period_of_interest.year}-{period_of_interest.month:02d}'
    coverage_tif_filename = f'{spi_part}__{living_lab}__{time_part}.tif'
    
    if data_type == 'historic':
        s3_uri = os.path.join(_s3_bucket, 'spi_data', 'historic', coverage_tif_filename)
    elif data_type == 'forecast':
        s3_uri = os.path.join(_s3_bucket, 'spi_data', 'forecast', coverage_tif_filename)
    else:
        raise ProcessorExecuteError('data_type must be one of ["forecast", "historic"]')
    return s3_uri 


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
        shutil.make_archive(coverage_zarr_zip_filepath, "zip", coverage_zarr_filepath)  # Comprimo in .zip per trasmetterlo come byte
        with open(coverage_zarr_zip_filepath, "rb") as f:
            zarr_bytes = f.read()
        coverage_out = str(zarr_bytes)
        
    LOGGER.debug(f'SPI coverage converted in {out_format} format')
    return coverage_out
    
    
    
def build_output_response(periods_of_interest, out_spi_coverages):
    return {
        'spi_coverage_info': [
            {
                'period_of_interest': period_of_interest.strftime('%Y-%m'),
                'spi_coverage_data': out_spi_coverage
            } 
            for period_of_interest, out_spi_coverage 
            in zip(
                periods_of_interest,
                out_spi_coverages
            )
        ]
    }