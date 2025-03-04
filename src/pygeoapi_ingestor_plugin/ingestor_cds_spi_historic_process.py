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
from filelock import FileLock

import numpy as np
import pandas as pd

import scipy.stats as stats
from scipy.special import gammainc, gamma

import xarray as xr
import pygrib

import s3fs
import cdsapi

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

import pygeoapi_ingestor_plugin.utils_s3 as s3_utils
import pygeoapi_ingestor_plugin.utils_spi as spi_utils
import pygeoapi_ingestor_plugin.utils as utils


LOGGER = logging.getLogger(__name__)

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.2.0',
    'id': 'cds-ingestor-process',
    'title': {
        'en': 'SPI Historic Calculator Process',
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
        'token': {
            'title': 'secret token',
            'description': 'identify yourself',
            'schema': {
                'type': 'string'
            }
        },
        "living_lab": {
            "title": "Living Lab",
            "description": "The Living Lab name",
            "schema": {
            },
        },
        'period_of_interest': {
            'title': 'period of interest (YYYY-MM)',
            'description': 'Month in which we are interested in calculating the SPI. (Format YYYY-MM)',
            'schema': {
            }
        },
        'spi_ts': { # TODO: to be implemented in future timescales greater than 1 month
            'title': 'SPI timescale',
            'description': 'Time scale for the SPI calculation (SPI is calculated for each month with a moving window of selected time-length). "1" for 1 month. Greater timescales are not yet implemented.',
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
            "token": "ABC123XYZ666",
            "living_lab": "georgia",
            "period_of_interest": "2025-03",
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
            key = os.getenv('CDSAPI_KEY')
        )
        
        
            
    def query_poi_cds_data(self, living_lab, period_of_interest, spi_ts):
        """
        Query data from CDS API based on bbox range, period of interest and time scale.
        
        REF: https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land
        """
        
        # Format params for CDS API query    
        # period_of_interest = spi_utils.format_params_for_poi_cds_query(period_of_interest)    
        
        # Get (Years, Years-Months) couple for the CDS api query. (We can query just one month at time)
        spi_start_date = period_of_interest[0] - relativedelta(months=spi_ts-1)
        spi_years_range = list(range(spi_start_date.year, period_of_interest[1].year+1))
        spi_month_range = []
        for iy,year in enumerate(range(spi_years_range[0], spi_years_range[-1]+1)):
            if iy==0 and len(spi_years_range)==1:
                spi_month_range.append([month for month in range(spi_start_date.month, period_of_interest[1].month+1)])
            elif iy==0 and len(spi_years_range)>1:
                spi_month_range.append([month for month in range(spi_start_date.month, 13)])
            elif iy>0 and iy==len(spi_years_range)-1:
                spi_month_range.append([month for month in range(1, period_of_interest[1].month+1)])
            else:
                spi_month_range.append([month for month in range(1, 13)])
        
        # Build CDS query response filepath
        def build_cds_hourly_data_filepath(year, month):
            dataset_part = 'reanalysis_era5_land__total_precipitation__hourly'
            time_part = f'{year}-{month[0]:02d}_{year}-{month[-1]:02d}'
            filename = f'{dataset_part}__{living_lab}__{time_part}.grib'
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
                        utils.ceil_decimals(spi_utils._living_lab_bbox[living_lab]['max_y'], 1),    # N
                        utils.floor_decimals(spi_utils._living_lab_bbox[living_lab]['min_x'], 1),   # W
                        utils.floor_decimals(spi_utils._living_lab_bbox[living_lab]['min_y'], 1),   # S
                        utils.ceil_decimals(spi_utils._living_lab_bbox[living_lab]['max_x'], 1),    # E
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
        cds_poi_data = cds_poi_data.sel(time=(cds_poi_data.time.dt.date>=period_of_interest[0]) & (cds_poi_data.time.dt.date<=period_of_interest[1]))
        
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
        
        month_spi_coverages = []
        for month in poi_dataset.time:
            month_spi_coverage = xr.apply_ufunc(
                lambda tile_timeseries: spi_utils.compute_timeseries_spi(tile_timeseries, spi_ts=spi_ts, nt_return=1), 
                cov_ts_dataset.sel(time=cov_ts_dataset.time<=month).tp.sortby('time'),
                input_core_dims = [['time']],
                vectorize = True
            )
            month_spi_coverages.append((
                month.dt.date.item(),
                month_spi_coverage        
            ))
        
        periods_of_interest = [spi_coverage[0] for spi_coverage in month_spi_coverages]
        month_spi_coverages = [spi_coverage[1] for spi_coverage in month_spi_coverages]
        
        LOGGER.debug('SPI coverage computed')
    
        return periods_of_interest, month_spi_coverages  
        

    def save_spi_coverage_to_collection(self, living_lab, spi_ts, periods_of_interest, month_spi_coverages):
        
        def build_data(spi_ts, periods_of_interest, month_spi_coverages):
            ds = xr.concat(month_spi_coverages, dim="time")
            ds = ds.assign_coords(time = [datetime.datetime.fromisoformat(p.isoformat()) for p in periods_of_interest])
            ds = ds.to_dataset()
            ds = ds.rename_vars({'tp': f'spi{spi_ts}'})
            return ds 
        
        ds = build_data(spi_ts, periods_of_interest, month_spi_coverages)        
        collection_params = spi_utils.create_s3_collection_data(living_lab, ds, data_type='historic')
        spi_utils.update_config(living_lab, collection_params)
        return {
            'collection_id': collection_params['collection_pygeoapi_id'],
            'collection_data': collection_params['data']
        }
    
    
    def execute(self, data):

        mimetype = 'application/json'
        
        try:
            
            # Validate request params
            living_lab, period_of_interest, spi_ts, out_format = spi_utils.validate_parameters(data, data_type='historic')
            
            # Gather needed data (Ref + PoI)
            ref_dataset = spi_utils.read_ref_cds_data(living_lab)
            poi_dataset = self.query_poi_cds_data(living_lab, period_of_interest, spi_ts)
            
            # Compute SPI coverage
            periods_of_interest, month_spi_coverages = self.compute_coverage_spi(ref_dataset, poi_dataset, spi_ts)
            
            # Save SPI coverage to collection
            collection_info = self.save_spi_coverage_to_collection(living_lab, spi_ts, periods_of_interest, month_spi_coverages)
            
            # Convert SPI coverage in the requested output format
            out_spi_coverages = spi_utils.coverages_to_out_format(month_spi_coverages, out_format)
            
            # Build output response with spi coverage data infos
            spi_coverage_response_info = spi_utils.build_output_response(periods_of_interest, out_spi_coverages)
            
            outputs = {
                'status': 'OK',
                ** collection_info,
                ** spi_coverage_response_info
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
