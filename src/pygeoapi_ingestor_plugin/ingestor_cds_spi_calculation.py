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
import datetime

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

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
        'spi_dataset': {
            'title': 'SPI dataset',
            'description': 'SPI coverage data',
            'schema': {
            }
        }
    },
    'example': {
        "inputs": {
            "debug": True,
            "lat_range": [41.120975, 41.12097],
            "long_range": [45.19624, 46.73688],
            "period_of_interest": "2025-01-32T00:00:00.000",
            "spi_ts": 1,
            "out_format": "netcdf"
        }
    }
}


class IngestorCDSSPICalculationProcessor(BaseProcessor):
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
        
        
    def validate_parameters(self, data):
        """
        Validate request parameters
        """
        lat_range = data.get('lat_range', None)
        long_range = data.get('long_range', None)
        period_of_interest = data.get('period_of_interest', None)
        spi_ts = data.get('spi_ts', None)
        out_format = data.get('out_format', None)
        
        if lat_range is None:
            raise ProcessorExecuteError('Cannot process without a lat_range')
        if type(lat_range) is not list or len(lat_range) != 2:
            raise ProcessorExecuteError('lat_range must be a list of 2 elements')
        if type(lat_range[0]) not in [int, float] or type(lat_range[1]) not in [int, float]:
            raise ProcessorExecuteError('lat_range elements must be float')
        if lat_range[0] < -90 or lat_range[0] > 90 or lat_range[1] < -90 or lat_range[1] > 90:
            raise ProcessorExecuteError('lat_range elements must be in the range [-90, 90]')
        if lat_range[0] > lat_range[1]:
            raise ProcessorExecuteError('lat_range[0] must be less than lat_range[1]')
        
        if long_range is None:
            raise ProcessorExecuteError('Cannot process without a long_range')
        if type(long_range) is not list or len(long_range) != 2:
            raise ProcessorExecuteError('long_range must be a list of 2 elements')
        if type(long_range[0]) not in [int, float] or type(long_range[1]) not in [int, float]:
            raise ProcessorExecuteError('long_range elements must be float')
        if long_range[0] < -180 or long_range[0] > 180 or long_range[1] < -180 or long_range[1] > 180:
            raise ProcessorExecuteError('long_range elements must be in the range [-180, 180]')
        if long_range[0] > long_range[1]:
            raise ProcessorExecuteError('long_range[0] must be less than long_range[1]')
        
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
        
        return lat_range, long_range, period_of_interest, spi_ts, out_format
        
        
        
        

    def execute(self, data):

        mimetype = 'application/json'
        
        try:
            
            # Validate request params
            lat_range, long_range, period_of_interest, spi_ts, out_format = self.validate_parameters(data)
            
            
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
        return f'<IngestorCDSSPICalculationProcessor> {self.name}'
