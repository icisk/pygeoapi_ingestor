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
import pygeoapi_ingestor_plugin.utils_spi as spi_utils


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
        
        
        # spi_utils._living_lab_bbox = {
        #     'georgia': [45.196243, 41.120975, 46.736885, 42.115760]
        # }
        
        # spi_utils._s3_bucket = f's3://saferplaces.co/test/icisk/spi/'
        # spi_utils._living_lab_s3_ref_data = {
        #     'georgia': os.path.join(spi_utils._s3_bucket, 'reference_data', 'era5_land__total_precipitation__georgia__monthly__1950_2025.nc')
        # }
        
        # self.reference_period = (datetime.datetime(1980, 1, 1), datetime.datetime(2010, 12, 31)) # REF: https://drought.emergency.copernicus.eu/data/factsheets/factsheet_spi.pdf
        
        # spi_utils._temp_dir = os.path.join(tempfile.gettempdir(), 'IngestorCDSSPIProcessProcessor')
        
        
            
    def query_poi_cds_data(self, living_lab, lat_range, long_range, period_of_interest, spi_ts):
        """
        Query data from CDS API based on bbox range, period of interest and time scale.
        
        REF: https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land
        """
        
        lat_range = lat_range if lat_range is not None else [spi_utils._living_lab_bbox[living_lab][1], spi_utils._living_lab_bbox[living_lab][3]]
        long_range = long_range if long_range is not None else [spi_utils._living_lab_bbox[living_lab][0], spi_utils._living_lab_bbox[living_lab][2]]
        
        
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
            bbox_part = f'{long_range[0]}_{lat_range[0]}_{long_range[1]}_{lat_range[1]}' if [long_range[0],lat_range[0],long_range[1],lat_range[1]] != spi_utils._living_lab_bbox[living_lab] else f'{living_lab}'
            time_part = f'{year}-{month[0]:02d}_{year}-{month[-1]:02d}'
            filename = f'{dataset_part}__{bbox_part}__{time_part}.grib'
            filedir = os.path.join(spi_utils._temp_dir, dataset_part)
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
           
        # Merge all the grib files in a single xarray dataset 
        cds_poi_datasets = []
        for cds_poi_data_filepath in cds_poi_data_filepaths:
            cds_poi_dataset = spi_utils.grib2xr(cds_poi_data_filepath, grib_var_name='Total precipitation', xr_var_name='tp')
            cds_poi_datasets.append(cds_poi_dataset)
        cds_poi_data = xr.concat(cds_poi_datasets, dim='time')
        cds_poi_data = cds_poi_data.sortby(['time', 'lat', 'lon'])
        
        LOGGER.debug('period of interest data read')
        return cds_poi_data        
    
    
    def compute_coverage_spi(self, ref_dataset, poi_dataset, spi_ts):
        """
        Compute SPI index for each tile in a lat-lon grid datasets.
        """
        
        def preprocess_poi_dataset(poi_dataset):
            poi_dataset = poi_dataset.resample(time='1ME').sum()                                   # Resample to monthly total data
            poi_dataset = poi_dataset.assign_coords(time=poi_dataset.time.dt.strftime('%Y-%m-01'))  # Set month day to 01
            poi_dataset = poi_dataset.assign_coords(time=pd.to_datetime(poi_dataset.time))
            poi_dataset['tp'] = poi_dataset['tp'] / 12                                              # Convert total precipitation to monthly average precipitation 
            poi_dataset = poi_dataset.assign_coords(
                lat=np.round(poi_dataset.lat.values, 6),
                lon=np.round(poi_dataset.lon.values, 6),
            )
            poi_dataset = poi_dataset.sortby(['time', 'lat', 'lon'])
            return poi_dataset        
        
        ref_dataset = spi_utils.preprocess_ref_dataset(ref_dataset)
        poi_dataset = preprocess_poi_dataset(poi_dataset).interp(lat=ref_dataset.lat, lon=ref_dataset.lon, method='linear')
        
        cov_ts_dataset = xr.concat([ref_dataset, poi_dataset], dim='time')
        cov_ts_dataset = cov_ts_dataset.drop_duplicates(dim='time').sortby(['time', 'lat', 'lon'])
        
        spi_coverage = xr.apply_ufunc(
            lambda tile_timeseries: spi_utils.compute_timeseries_spi(tile_timeseries, spi_ts=spi_ts, nt_return=1), 
            cov_ts_dataset.tp.sortby('time'),
            input_core_dims = [['time']],
            vectorize = True
        )
        
        LOGGER.debug('SPI coverage computed')
        return spi_coverage
        

    def execute(self, data):

        mimetype = 'application/json'
        
        try:
            
            # Validate request params
            living_lab, lat_range, long_range, period_of_interest, spi_ts, out_format = spi_utils.validate_parameters(data, data_type='historic')
            
            # Gather needed data (Ref + PoI)
            ref_dataset = spi_utils.read_ref_cds_data(living_lab, lat_range, long_range)
            poi_dataset = self.query_poi_cds_data(living_lab, lat_range, long_range, period_of_interest, spi_ts)
            
            # Compute SPI coverage
            spi_coverage = self.compute_coverage_spi(ref_dataset, poi_dataset, spi_ts)
            
            # Save SPI coverage to file
            spi_coverage_s3_uri = spi_utils.build_spi_s3_uri(living_lab, lat_range, long_range, period_of_interest, spi_ts, data_type='historic')
            spi_utils.save_coverage_to_s3(spi_coverage, spi_coverage_s3_uri)
            
            # Save SPI coverage to collection
            # TODO: (Maybe) Save SPI coverage to collection
            
            # Convert SPI coverage in the requested output format
            out_spi_coverage = spi_utils.coverage_to_out_format(spi_coverage, out_format)
            
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
