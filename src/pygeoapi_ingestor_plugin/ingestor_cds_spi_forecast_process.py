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
from calendar import monthrange
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
            'description': 'Reference (range of) date(s) on which to calculate the index',
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
            "living_lab": "georgia",
            "lat_range": [ 41.120975, 42.115760 ],
            "long_range": [ 45.196243, 46.736885 ],
            "period_of_interest": "2025-01-31T00:00:00.000",
            "spi_ts": 1,
            "out_format": "netcdf"
        }
    }
}


class IngestorCDSSPIForecastProcessProcessor(BaseProcessor):
    """
    Ingestor Processor

    Takes bbox range, a period of interest and a time_scale in month and returns the SPI calculation coverage data. 
    Uploads result to S3 and add coverage data to a related bbox and time_scale collection.
    """
    

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition

        :returns: pygeoapi.process.ingestor_process.IngestorCDSSPIForecastProcessProcessor
        """

        super().__init__(processor_def, PROCESS_METADATA)
        
        self.cds_client = cdsapi.Client(
            url = 'https://cds.climate.copernicus.eu/api',
            key = 'b6c439dd-22d4-4b39-bbf7-9e6e57d9ae0d' # TODO: os.getenv('CDSAPI_KEY')
        )
            
            
    def query_poi_cds_data(self, living_lab, lat_range, long_range, period_of_interest, spi_ts):
        """
        Query data from CDS API based on bbox range, period of interest and time scale.
        
        REF: https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land
        """
        
        days_in_month = lambda date: monthrange(date.year, date.month)[1]
        
        lat_range = lat_range if lat_range is not None else [spi_utils._living_lab_bbox[living_lab][1], spi_utils._living_lab_bbox[living_lab][3]]
        long_range = long_range if long_range is not None else [spi_utils._living_lab_bbox[living_lab][0], spi_utils._living_lab_bbox[living_lab][2]]
        
        if len(period_of_interest) == 1:
            period_of_interest = [
                period_of_interest[0].date().replace(day=1),
                period_of_interest[0].date().replace(day=days_in_month(period_of_interest[0]))
            ]
        else:
            period_of_interest = [
                period_of_interest[0].date().replace(day=1),
                period_of_interest[1].date().replace(day=days_in_month(period_of_interest[1]))
            ]
        
        # Build CDS query response filepath
        def build_cds_hourly_data_filepath(start_year, start_month, end_year, end_month):            
            dataset_part = 'seasonal_original_single_levels__total_precipitation__daily'
            bbox_part = f'{long_range[0]}_{lat_range[0]}_{long_range[1]}_{lat_range[1]}' if [long_range[0],lat_range[0],long_range[1],lat_range[1]] != spi_utils._living_lab_bbox[living_lab] else f'{living_lab}'
            time_part = f'{start_year}-{start_month:02d}' if start_year==end_year and start_month==end_month else f'{start_year}-{start_month:02d}_{end_year}-{end_month:02d}'
            filename = f'{dataset_part}__{bbox_part}__{time_part}.nc'
            filedir = os.path.join(spi_utils._temp_dir, dataset_part)
            if not os.path.exists(filedir):
                os.makedirs(filedir, exist_ok=True)
            filepath = os.path.join(filedir, filename)
            return filepath
        
        # CDS API query    
        init_date = datetime.datetime.now()
        start_hour = max(24, (period_of_interest[0] - init_date.date()).days*24)
        end_hour = min(5160, (period_of_interest[1] - period_of_interest[0]).days*24)

        cds_poi_data_filepath = build_cds_hourly_data_filepath(period_of_interest[0].year, period_of_interest[0].month, period_of_interest[1].year, period_of_interest[1].month)       
                
        if not os.path.exists(cds_poi_data_filepath):
            cds_dataset = "seasonal-original-single-levels"
            cds_query = {
                "originating_centre": "ecmwf",
                "system": "51",
                "variable": [
                    "total_precipitation"
                ],
                "year": [str(init_date.year)],
                "month": [f'{init_date.month:02d}'],
                "day": ["01"],
                "leadtime_hour": [str(h) for h in range(start_hour, end_hour+24, 24)],
                "area": [
                    math.ceil(lat_range[1]),   # N
                    math.floor(long_range[0]),  # W
                    math.floor(lat_range[0]),   # S
                    math.ceil(long_range[1])   # E
                ],
                "data_format": "netcdf",
            }
            self.cds_client.retrieve(cds_dataset, cds_query, cds_poi_data_filepath)
           
        cds_poi_data = xr.open_dataset(cds_poi_data_filepath)
        cds_poi_data = xr.Dataset(
            {
                'tp': (['r', 'time', 'lat', 'lon'], cds_poi_data.tp.values[:,0,:,:,:])
            },
            coords={
                'r': np.arange(0,len(cds_poi_data.number),1),
                'time': cds_poi_data.valid_time.values,
                'lat': cds_poi_data.latitude.values,
                'lon': cds_poi_data.longitude.values
            }
        )
        
        LOGGER.debug('period of interest data read')
        return cds_poi_data      
    
    
    def compute_coverage_spi(self, ref_dataset, poi_dataset, spi_ts):
        """
        Compute SPI index for each tile in a lat-lon grid datasets.
        """
        
        def preprocess_poi_dataset(poi_dataset):
            poi_dataset = poi_dataset.resample(time='1ME').mean()                                  # Resample to monthly total data
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
        poi_dataset = preprocess_poi_dataset(poi_dataset).interp(lat=ref_dataset.lat, lon=ref_dataset.lon)
        
        cov_ts_dataset = xr.concat([ref_dataset, poi_dataset], dim='time')
        cov_ts_dataset = cov_ts_dataset.drop_duplicates(dim='time').sortby(['time', 'lat', 'lon'])
        
        month_spi_coverages = []
        for month in poi_dataset.time:
            r_spi_coverages = []
            for r_id in cov_ts_dataset.r:
                r_spi_coverage = xr.apply_ufunc(
                    lambda tile_timeseries: spi_utils.compute_timeseries_spi(tile_timeseries, spi_ts=spi_ts, nt_return=1), 
                    cov_ts_dataset.sel(r=r_id, time=cov_ts_dataset.time<=month).tp.sortby('time'),
                    input_core_dims = [['time']],
                    vectorize = True
                )
                r_spi_coverages.append(r_spi_coverage)
            
            month_spi_coverage = xr.concat(r_spi_coverages, dim="r")
            month_spi_coverage = month_spi_coverage.assign_coords(r=range(0, len(r_spi_coverages)))
            month_spi_coverages.append((
                month.dt.date.item(),
                month_spi_coverage        
            ))
        
        periods_of_interest = [spi_coverage[0] for spi_coverage in month_spi_coverages]
        month_spi_coverages = [spi_coverage[1] for spi_coverage in month_spi_coverages]
        
        LOGGER.debug('SPI coverage computed')
        return periods_of_interest, month_spi_coverages  
    
    
    def build_spi_s3_uris(self, living_lab, lat_range, long_range, periods_of_interest, spi_ts):
        s3_uris = []
        for period_of_interest in periods_of_interest:
            s3_uris.append(spi_utils.build_spi_s3_uri(living_lab, lat_range, long_range, period_of_interest, spi_ts, data_type='forecast'))
        return s3_uris
    
    
    def save_coverages_to_s3(self, coverages, s3_uris):
        for coverage, s3_uri in zip(coverages, s3_uris):
            _ = spi_utils.save_coverage_to_s3(coverage, s3_uri)
            
    
    def coverages_to_out_format(self, coverages, out_format):
        out_coverages = []
        for coverage in coverages:
            out_coverages.append(spi_utils.coverage_to_out_format(coverage, out_format))
        return out_coverages
        
        
    
    def execute(self, data):

        mimetype = 'application/json'
        
        try:
            # Validate request params
            living_lab, lat_range, long_range, period_of_interest, spi_ts, out_format = spi_utils.validate_parameters(data, data_type='forecast')      
            
            # Gather needed data (Ref + PoI)
            ref_dataset = spi_utils.read_ref_cds_data(living_lab, lat_range, long_range)
            poi_dataset = self.query_poi_cds_data(living_lab, lat_range, long_range, period_of_interest, spi_ts)
            
            # Compute SPI coverage
            periods_of_interest, month_spi_coverages = self.compute_coverage_spi(ref_dataset, poi_dataset, spi_ts)
            
            # # INFO: just for test
            # for p,c in zip(periods_of_interest, month_spi_coverages):
            #     c.to_netcdf(os.path.join(spi_utils._temp_dir, f'{p}.nc'))
            
            # Save SPI coverage to file
            spi_coverage_s3_uris = self.build_spi_s3_uris(living_lab, lat_range, long_range, periods_of_interest, spi_ts)
            self.save_coverages_to_s3(month_spi_coverages, spi_coverage_s3_uris)
            
            # Save SPI coverage to collection
            # TODO: (Maybe) Save SPI coverage to collection
            
            # Convert SPI coverage in the requested output format
            out_spi_coverages = self.coverages_to_out_format(month_spi_coverages, out_format)
            
            outputs = {
                'status': 'OK',
                'spi_coverage_info': [
                    {
                        'period_of_interest': period_of_interest.strftime('%Y-%m'),
                        'spi_coverage_s3_uri': spi_coverage_s3_uris[poi_idx],
                        'spi_coverage_data': out_spi_coverages[poi_idx]
                    } 
                    for poi_idx, period_of_interest in enumerate(periods_of_interest)
                ]
            }
            
        except Exception as err:
            outputs = {
                'status': 'KO',
                'error': str(err)
            }
            raise ProcessorExecuteError(str(err))
        
        return mimetype, outputs

    def __repr__(self):
        return f'<IngestorCDSSPIForecastProcessProcessor> {self.name}'