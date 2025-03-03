import os
import logging, time

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

# =================================================================
#
# Authors: Tommaso Redaelli <tommaso.redaelli@gecosistema.com>
#
# Copyright (c) 2023 Tommaso Redaelli
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

LOGGER = logging.getLogger(__name__)

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.2.0',
    'id': 'safer-process',
    'title': {
        'en': 'Bias correction procedure',
    },
    'description': {
        'en': 'Bias correction procedure for ECMWF forecast temperature and precipitation data in Spain',
    },
    'jobControlOptions': ['sync-execute', 'async-execute'],
    'keywords': ['safer process'],
    'inputs': {
        'period_of_interest': {
            'title': 'period of interest',
            'description': 'First date is interpreted as init month and second date as last valid forecast month (this can\'t be more than six month from the first date)',
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
        'data': {
            'title': 'data',
            'description': 'The data output file'
        }
    },
    'example': {
        "inputs": {
            "debug": True,
            "period_of_interest": "2025-01-31T00:00:00.000",
            "out_format": "netcdf"
        }
    }
}


class BiasCorrectionProcessor(BaseProcessor):
    """
    Bias Correction Processor
    """

    def __init__(self, processor_def):

        super().__init__(processor_def, PROCESS_METADATA)
        
        
        
        

    def execute(self, data):

        mimetype = 'application/json'

        # TODO: Do something with the data
        outputs = {
            'status': 'OK',
        }
        return mimetype, outputs

    def __repr__(self):
        return f'<BiasCorrectionProcessor> {self.name}'
