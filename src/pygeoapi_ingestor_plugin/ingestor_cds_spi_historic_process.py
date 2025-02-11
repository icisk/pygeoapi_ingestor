# =================================================================
#
# Authors: Valerio Luzzi <valluzzi@gmail.com>
#
# Copyright (c) 2023 Valerio Luzzi
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

import os
import logging, time
import math
import json
import shutil
import datetime
from dateutil.relativedelta import relativedelta
import tempfile

import numpy as np
import pandas as pd

import scipy.stats as stats
from scipy.special import gammainc, gamma

import xarray as xr
import pygrib

import cdsapi

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

import pygeoapi_ingestor_plugin.utils_s3 as s3_utils


LOGGER = logging.getLogger(__name__)

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.2.0',
    'id': 'cds-ingestor-process',
    'title': {
        'en': 'CDS Ingestor Process',
    },
    'description': {
        'en': 'Ingestor process for Copernicus Climate Data Store (CDS) data',},
    'jobControlOptions': ['sync-execute', 'async-execute'],
    'keywords': ['ingestor process','cds'],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'information',
        'href': 'https://example.org/process',
        'hreflang': 'en-US'
    }],
    'inputs': {
        "living_lab": {
            "title": "Living Lab",
            "description": "The Living Lab name",
            "schema": {
            },
        },
        'lat_range': {
            'title': 'latitude range',
            'description': 'The latitude range',
            'schema': {
            }
        },
        'long_range': {
            'title': 'longitude range',
            'description': 'The longitude range',
            'schema': {
            }
        },
        'period_of_interest': {
            'title': 'period of interest',
            'description': 'Reference date on which to calculate the index',
            'schema': {
            }
        },
        'spi_ts': {
            'title': 'SPI timescale',
            'description': 'Time scale for the SPI calculation (SPI is calculated for each month with a moving window of selected time-length). It could be 1,3,6,12,24,48 months',
            'schema': {
            }
        },
        'out_format': {
            'title': 'Return format type',
            'description': 'The return format type.',
            'schema': {
            }
        }, 
        'debug': {
            'title': 'Debug',
            'description': 'Enable Debug mode',
            'schema': {
            }
        }
    },
    'outputs': {
        'status': {
            'title': 'status',
            'description': 'Staus of the process execution [OK or KO]',
            'schema': {
            }
        },
        'spi_coverage_s3_uri': {
            'title': 'SPI coverage S3 URI',
            'description': 'SPI coverage S3 URI',
            'schema': {
            }  
        },
        'spi_coverage_data': {
            'title': 'SPI dataset',
            'description': 'SPI coverage data',
            'schema': {
            }
        }
    },
    'example': {
        "inputs": {
            "debug": True,
            "living_lab": "georgia",
            "lat_range": [ 41.120975, 42.115760 ],
            "long_range": [ 45.196243, 46.736885 ],
            "period_of_interest": "2025-01-31T00:00:00.000",
            "spi_ts": 1,
            "out_format": "netcdf"
        }
    }
}


class IngestorCDSSPIHistoricProcessProcessor(BaseProcessor):
    """
    Ingestor Processor

    Takes bbox range, a period of interest and a time_scale in month and returns the SPI calculation coverage data. 
    Uploads result to S3 and add coverage data to a related bbox and time_scale collection.
    """
    

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition

        :returns: pygeoapi.process.ingestor_process.IngestorCDSSPICalculationProcessor
        """

        super().__init__(processor_def, PROCESS_METADATA)
        
        self.cds_client = cdsapi.Client(
            url = 'https://cds.climate.copernicus.eu/api',
            key = 'b6c439dd-22d4-4b39-bbf7-9e6e57d9ae0d' # TODO: os.getenv('CDSAPI_KEY')
        )
        
        
        self.living_lab_bbox = {
            'georgia': [45.196243, 41.120975, 46.736885, 42.115760]
        }
        
        self.s3_bucket = f's3://saferplaces.co/test/icisk/spi/'
        self.living_lab_s3_ref_data = {
            'georgia': os.path.join(self.s3_bucket, 'reference_data', 'era5_land__total_precipitation__georgia__monthly__1950_2025.nc')
        }
        
        self.reference_period = (datetime.datetime(1980, 1, 1), datetime.datetime(2010, 12, 31)) # REF: https://drought.emergency.copernicus.eu/data/factsheets/factsheet_spi.pdf
        
        self.temp_dir = os.path.join(tempfile.gettempdir(), 'IngestorCDSSPIProcessProcessor')
        if not os.path.exists(self.temp_dir):
            os.makedirs(self.temp_dir, exist_ok=True)
        
        
    def validate_parameters(self, data):
        """
        Validate request parameters
        """
        
        living_lab = data.get('living_lab', None)
        lat_range = data.get('lat_range', None)
        long_range = data.get('long_range', None)
        period_of_interest = data.get('period_of_interest', None)
        spi_ts = data.get('spi_ts', None)
        out_format = data.get('out_format', None)
        
        if living_lab is None:
            raise ProcessorExecuteError('Cannot process without a living_lab valued')
        if type(living_lab) is not str:
            raise ProcessorExecuteError('living_lab must be a string')
        if living_lab not in self.living_lab_bbox.keys():
            raise ProcessorExecuteError(f'living_lab must be one of {[f"{ll}" for ll in list(self.living_lab_bbox.keys())]})')
        
        if lat_range is not None:
            if type(lat_range) is not list or len(lat_range) != 2:
                raise ProcessorExecuteError('lat_range must be a list of 2 elements')
            if type(lat_range[0]) not in [int, float] or type(lat_range[1]) not in [int, float]:
                raise ProcessorExecuteError('lat_range elements must be float')
            if lat_range[0] < -90 or lat_range[0] > 90 or lat_range[1] < -90 or lat_range[1] > 90:
                raise ProcessorExecuteError('lat_range elements must be in the range [-90, 90]')
            if lat_range[0] > lat_range[1]:
                raise ProcessorExecuteError('lat_range[0] must be less than lat_range[1]')
            if lat_range[0] < self.living_lab_bbox[living_lab][1] or lat_range[0] > self.living_lab_bbox[living_lab][3] or \
                lat_range[1] < self.living_lab_bbox[living_lab][1] or lat_range[1] > self.living_lab_bbox[living_lab][3]:
                raise ProcessorExecuteError(f'lat_range must be in the living_lab bbox range: [{self.living_lab_bbox[living_lab][1]} ; {self.living_lab_bbox[living_lab][3]}]')
        
        if long_range is not None:
            if type(long_range) is not list or len(long_range) != 2:
                raise ProcessorExecuteError('long_range must be a list of 2 elements')
            if type(long_range[0]) not in [int, float] or type(long_range[1]) not in [int, float]:
                raise ProcessorExecuteError('long_range elements must be float')
            if long_range[0] < -180 or long_range[0] > 180 or long_range[1] < -180 or long_range[1] > 180:
                raise ProcessorExecuteError('long_range elements must be in the range [-180, 180]')
            if long_range[0] > long_range[1]:
                raise ProcessorExecuteError('long_range[0] must be less than long_range[1]')
            if long_range[0] < self.living_lab_bbox[living_lab][0] or long_range[0] > self.living_lab_bbox[living_lab][2] or \
                long_range[1] < self.living_lab_bbox[living_lab][0] or long_range[1] > self.living_lab_bbox[living_lab][2]:
                raise ProcessorExecuteError(f'long_range must be in the living_lab bbox range: [{self.living_lab_bbox[living_lab][0]} ; {self.living_lab_bbox[living_lab][2]}]')
        
        if period_of_interest is None:
            raise ProcessorExecuteError('Cannot process without a period_of_interest valued')
        if type(period_of_interest) is not str:
            raise ProcessorExecuteError('period_of_interest must be a string')
        if type(period_of_interest) is str:
            try:
                period_of_interest = datetime.datetime.fromisoformat(period_of_interest)
                if period_of_interest.strftime("%Y-%m") >= datetime.datetime.now().strftime("%Y-%m"):
                    raise ProcessorExecuteError('period_of_interest must be a date before current date month')                
            except ValueError:
                raise ProcessorExecuteError('period_of_interest must be a valid datetime iso-format string')
            
        if spi_ts is None:
            raise ProcessorExecuteError('Cannot process without a spi_ts valued')
        if type(spi_ts) is not int:
            raise ProcessorExecuteError('spi_ts must be an integer')
        if spi_ts not in [1,3,6,12,24,48]:
            raise ProcessorExecuteError('spi_ts must be 1,3,6,12,24,48')
        
        if out_format is None:
            out_format = 'netcdf'
        if type(out_format) is not str:
            raise ProcessorExecuteError('out_format must be a string or null')
        if out_format not in ['netcdf', 'json', 'dataframe', 'tif', 'zarr']:
            raise ProcessorExecuteError('out_format must be one of ["netcdf", "json", "dataframe", "tif", "zarr"]')
        
        LOGGER.debug('parameters validated')
        return living_lab, lat_range, long_range, period_of_interest, spi_ts, out_format
        
        
    def read_ref_cds_data(self, living_lab, lat_range, long_range):   
        """
        Read reference data from S3. 
        Slice them in the bbox range and a default reference period.
        
        REF: https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land-monthly-means
        """
        
        cds_ref_data_filepath = s3_utils.s3_download(
            uri = self.living_lab_s3_ref_data[living_lab],
            fileout = os.path.join(self.temp_dir, os.path.basename(self.living_lab_s3_ref_data[living_lab]))
        )
        
        cds_ref_data = xr.open_dataset(cds_ref_data_filepath) 
        cds_ref_data = cds_ref_data.sortby(['time', 'lat', 'lon'])
        cds_ref_data = cds_ref_data.sel(
            lat = slice(*lat_range if lat_range is not None else (None, None)),
            lon = slice(*long_range if long_range is not None else (None, None)),
            time = slice(*self.reference_period)
        )
        
        LOGGER.debug('reference data read')
        return cds_ref_data
    
    
    def query_poi_cds_data(self, living_lab, lat_range, long_range, period_of_interest, spi_ts):
        """
        Query data from CDS API based on bbox range, period of interest and time scale.
        
        REF: https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land
        """
        
        lat_range = lat_range if lat_range is not None else [self.living_lab_bbox[living_lab][1], self.living_lab_bbox[living_lab][3]]
        long_range = long_range if long_range is not None else [self.living_lab_bbox[living_lab][0], self.living_lab_bbox[living_lab][2]]
        
        
        # Get (Years, Years-Months) couple for the CDS api query. (We can query just one month at time)
        spi_start_date = period_of_interest - relativedelta(months=spi_ts-1)
        spi_years_range = list(range(spi_start_date.year, period_of_interest.year+1))
        spi_month_range = []
        for iy,year in enumerate(range(spi_years_range[0], spi_years_range[-1]+1)):
            if iy==0 and len(spi_years_range)==1:
                spi_month_range.append([month for month in range(spi_start_date.month, period_of_interest.month+1)])
            elif iy==0 and len(spi_years_range)>1:
                spi_month_range.append([month for month in range(spi_start_date.month, 13)])
            elif iy>0 and iy==len(spi_years_range)-1:
                spi_month_range.append([month for month in range(1, period_of_interest.month+1)])
            else:
                spi_month_range.append([month for month in range(1, 13)])
        
        # Build CDS query response filepath
        def build_cds_hourly_data_filepath(year, month):
            dataset_part = 'reanalysis_era5_land__total_precipitation__hourly'
            bbox_part = f'{long_range[0]}_{lat_range[0]}_{long_range[1]}_{lat_range[1]}' if [long_range[0],lat_range[0],long_range[1],lat_range[1]] != self.living_lab_bbox[living_lab] else f'{living_lab}'
            time_part = f'{year}-{month[0]:02d}_{year}-{month[-1]:02d}'
            filename = f'{dataset_part}__{bbox_part}__{time_part}.grib'
            filedir = os.path.join(self.temp_dir, dataset_part)
            if not os.path.exists(filedir):
                os.makedirs(filedir, exist_ok=True)
            filepath = os.path.join(filedir, filename)
            return filepath
        
        # CDS API query    
        cds_poi_data_filepaths = []    
        for q_idx, (year,year_months) in enumerate(zip(spi_years_range, spi_month_range)):
            cds_poi_data_filepath = build_cds_hourly_data_filepath(year, year_months)       
            
            if not os.path.exists(cds_poi_data_filepath):
                cds_dataset = 'reanalysis-era5-land'
                cds_query =  {
                    'variable': 'total_precipitation',
                    'year': [str(year)],
                    'month': [f'{month:02d}' for month in year_months],
                    'day': [f'{day:02d}' for day in range(1, 32)],
                    'time': [f'{hour:02d}:00' for hour in range(0, 24)],
                    'area': [
                        lat_range[1],   # N
                        long_range[0],  # W
                        lat_range[0],   # S
                        long_range[1]   # E
                    ],
                    "data_format": "grib",
                    "download_format": "unarchived"
                }
                self.cds_client.retrieve(cds_dataset, cds_query, cds_poi_data_filepath)
                
            LOGGER.debug(f'{q_idx+1}/{len(spi_years_range)} - CDS API query completed')            
            cds_poi_data_filepaths.append(cds_poi_data_filepath)
           
        # Convert grib files to xarray dataset 
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
           
        # Merge all the grib files in a single xarray dataset 
        cds_poi_datasets = []
        for cds_poi_data_filepath in cds_poi_data_filepaths:
            cds_poi_dataset = grib2xr(cds_poi_data_filepath, grib_var_name='Total precipitation', xr_var_name='tp')
            cds_poi_datasets.append(cds_poi_dataset)
        cds_poi_data = xr.concat(cds_poi_datasets, dim='time')
        cds_poi_data = cds_poi_data.sortby(['time', 'lat', 'lon'])
        
        LOGGER.debug('period of interest data read')
        return cds_poi_data        
    
    
    def compute_timeseries_spi(self, monthly_data, spi_ts, nt_return=1):
        """
        Compute SPI index for a time series of monthly data
        
        REF: https://drought.emergency.copernicus.eu/data/factsheets/factsheet_spi.pdf
        REF: https://mountainscholar.org/items/842b69e8-a465-4aeb-b7ec-021703baa6af [ page 18 to 24 ]
        """
        
        df = pd.DataFrame({'monthly_data': monthly_data})

        # Totalled data over t_scale rolling windows
        t_scaled_monthly_data = df.rolling(spi_ts).sum().monthly_data.iloc[spi_ts:]

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
    
    
    def compute_coverage_spi(self, ref_dataset, poi_dataset, spi_ts):
        """
        Compute SPI index for each tile in a lat-lon grid datasets.
        """
        
        def preprocess_ref_dataset(ref_dataset):
            ref_dataset = ref_dataset * ref_dataset['time'].dt.days_in_month                        # Convert total precipitation to monthly total precipitation
            ref_dataset = ref_dataset.assign_coords(
                lat=np.round(ref_dataset.lat.values, 6),
                lon=np.round(ref_dataset.lon.values, 6),
            )
            ref_dataset = ref_dataset.sortby(['time', 'lat', 'lon'])
            return ref_dataset
        
        def preprocess_poi_dataset(poi_dataset):
            poi_dataset = poi_dataset.resample(time='1ME').sum()                                     # Resample to monthly total data
            poi_dataset = poi_dataset.assign_coords(time=poi_dataset.time.dt.strftime('%Y-%m-01'))  # Set month day to 01
            poi_dataset = poi_dataset.assign_coords(time=pd.to_datetime(poi_dataset.time))
            poi_dataset['tp'] = poi_dataset['tp'] / 12                                              # Convert total precipitation to monthly average precipitation 
            poi_dataset = poi_dataset.assign_coords(
                lat=np.round(poi_dataset.lat.values, 6),
                lon=np.round(poi_dataset.lon.values, 6),
            )
            poi_dataset = poi_dataset.sortby(['time', 'lat', 'lon'])
            return poi_dataset        
        
        ref_dataset = preprocess_ref_dataset(ref_dataset)
        poi_dataset = preprocess_poi_dataset(poi_dataset).interp(lat=ref_dataset.lat, lon=ref_dataset.lon)
        
        cov_ts_dataset = xr.concat([ref_dataset, poi_dataset], dim='time')
        cov_ts_dataset = cov_ts_dataset.drop_duplicates(dim='time').sortby(['time', 'lat', 'lon'])
        
        spi_coverage = xr.apply_ufunc(
            lambda tile_timeseries: self.compute_timeseries_spi(tile_timeseries, spi_ts=spi_ts, nt_return=1), cov_ts_dataset.tp.sortby('time'),
            input_core_dims = [['time']],
            vectorize = True
        )
        
        LOGGER.debug('SPI coverage computed')
        return spi_coverage
    
    
    def build_spi_s3_uri(self, living_lab, lat_range, long_range, period_of_interest, spi_ts):
        lat_range = lat_range if lat_range is not None else [self.living_lab_bbox[living_lab][1], self.living_lab_bbox[living_lab][3]]
        long_range = long_range if long_range is not None else [self.living_lab_bbox[living_lab][0], self.living_lab_bbox[living_lab][2]]
        
        bbox_part = f'{long_range[0]}_{lat_range[0]}_{long_range[1]}_{lat_range[1]}' if [long_range[0],lat_range[0],long_range[1],lat_range[1]] != self.living_lab_bbox[living_lab] else f'{living_lab}'
        spi_part = f'spi-{spi_ts}'
        time_part = f'{period_of_interest.year}-{period_of_interest.month:02d}'
        coverage_tif_filename = f'{spi_part}__{bbox_part}__{time_part}.tif'
        
        s3_uri = os.path.join(self.s3_bucket, 'spi_data', 'historic', coverage_tif_filename)
        return s3_uri
        
    
    def save_coverage_to_s3(self, coverage_ds, coverage_uri):
        """
        Save SPI coverage to S3
        """
        
        coverage_tif_filename = os.path.basename(coverage_uri)
        coverage_tif_filepath = os.path.join(self.temp_dir, coverage_tif_filename)
        coverage_ds.rio.write_crs("EPSG:4326", inplace=True)
        coverage_ds.rio.set_spatial_dims(x_dim='lon', y_dim='lat', inplace=True)
        coverage_ds.rio.to_raster(coverage_tif_filepath)
        
        _ = s3_utils.s3_upload(
            filename = coverage_tif_filepath,
            uri = coverage_uri
        )
    
    
    def coverage_to_out_format(self, coverage_ds, out_format):
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
            coverage_tif_filepath = os.path.join(self.temp_dir, 'spi_coverage.tif')
            coverage_ds.rio.write_crs("EPSG:4326", inplace=True)
            coverage_ds.rio.set_spatial_dims(x_dim='lon', y_dim='lat', inplace=True)
            coverage_ds.rio.to_raster(coverage_tif_filepath)
            with open(coverage_tif_filepath, "rb") as f:
                tif_bytes = f.read()
            coverage_out = str(tif_bytes)
        if out_format == 'zarr':
            coverage_zarr_filepath = os.path.join(self.temp_dir, 'spi_coverage.zarr')
            coverage_zarr_zip_filepath = os.path.join(self.temp_dir, 'spi_coverage_zarr')
            coverage_ds.to_zarr(coverage_zarr_filepath, mode="w")
            shutil.make_archive(coverage_zarr_zip_filepath, "zip", coverage_zarr_filepath)  # Comprimo in .zip per trasmetterlo come byte
            with open(coverage_zarr_zip_filepath, "rb") as f:
                zarr_bytes = f.read()
            coverage_out = str(zarr_bytes)
            
        LOGGER.debug(f'SPI coverage converted in {out_format} format')
        return coverage_out
        

    def execute(self, data):

        mimetype = 'application/json'
        
        try:
            
            # Validate request params
            living_lab, lat_range, long_range, period_of_interest, spi_ts, out_format = self.validate_parameters(data)
            
            # Gather needed data (Ref + PoI)
            ref_dataset = self.read_ref_cds_data(living_lab, lat_range, long_range)
            poi_dataset = self.query_poi_cds_data(living_lab, lat_range, long_range, period_of_interest, spi_ts)
            
            # # Compute SPI coverage
            spi_coverage = self.compute_coverage_spi(ref_dataset, poi_dataset, spi_ts)
            
            # Save SPI coverage to file
            spi_coverage_s3_uri = self.build_spi_s3_uri(living_lab, lat_range, long_range, period_of_interest, spi_ts)
            self.save_coverage_to_s3(spi_coverage, spi_coverage_s3_uri)
            
            # Save SPI coverage to collection
            # TODO: (Maybe) Save SPI coverage to collection
            
            # Convert SPI coverage in the requested output format
            out_spi_coverage = self.coverage_to_out_format(spi_coverage, out_format)
            
            outputs = {
                'status': 'OK',
                'spi_coverage_s3_uri': spi_coverage_s3_uri,
                'spi_coverage_data': out_spi_coverage
            }
            
        except Exception as err:
            outputs = {
                'status': 'KO',
                'error': str(err)
            }
            raise ProcessorExecuteError(str(err))
        
        return mimetype, outputs

    def __repr__(self):
        return f'<IngestorCDSSPIHistoricProcessProcessor> {self.name}'
