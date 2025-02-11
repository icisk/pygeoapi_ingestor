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
            'description': 'Reference (range of) date(s) on which to calculate the index',
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
            
    
    
    def execute(self, data):

        mimetype = 'application/json'
        
        try:
            
            outputs = {
                'status': 'OK'
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