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
import logging
import datetime
from dateutil.relativedelta import relativedelta

import numpy as np
import pandas as pd

import xarray as xr

import cdsapi

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

import pygeoapi_ingestor_plugin.utils_spi as spi_utils
import pygeoapi_ingestor_plugin.utils as utils


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
            'description': 'Init forecast month in which we are interested in calculating the SPI for next 6 month. (Format YYYY-MM)',
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
            'description': 'Status of the process execution [OK or KO]',
            'schema': {
            }
        },
        'spi_coverage_collection_s3_uri': {
            'title': 'SPI coverage S3 URI',
            'description': 'SPI coverage S3 URI',
            'schema': {
            }
        },
        'spi_coverage_collection_id': {
            'title': 'SPI coverage collection ID',
            'description': 'SPI coverage collection PyGeoApi ID',
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
            "period_of_interest": "2025-01",
            "spi_ts": 1
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
            key = os.getenv('CDSAPI_KEY')
        )


    def query_poi_cds_data(self, living_lab, period_of_interest, spi_ts):
        """
        Query data from CDS API based on bbox range, period of interest and time scale.

        REF: https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land
        """

        # Format params for CDS API query
        # period_of_interest = spi_utils.format_params_for_poi_cds_query(period_of_interest)

        # Build CDS query response filepath
        def build_cds_hourly_data_filepath(start_year, start_month, end_year, end_month):
            dataset_part = 'seasonal_original_single_levels__total_precipitation__daily'
            time_part = f'{start_year}-{start_month:02d}' if start_year==end_year and start_month==end_month else f'{start_year}-{start_month:02d}_{end_year}-{end_month:02d}'
            filename = f'{dataset_part}__{living_lab}__{time_part}.nc'
            filedir = os.path.join(spi_utils._temp_dir, dataset_part)
            if not os.path.exists(filedir):
                os.makedirs(filedir, exist_ok=True)
            filepath = os.path.join(filedir, filename)
            return filepath

        # CDS API query
        curr_date = datetime.datetime.now().date()
        if period_of_interest[0].strftime('%Y-%m') >= curr_date.strftime('%Y-%m'):
            if curr_date.day > 6: # Data is available from 7th day of the month
                init_date = datetime.datetime.now().replace(day=1).date()
            else:  # We can use forecast from previous init month
                init_date = (datetime.datetime.now() - relativedelta(months=1)).replace(day=1).date()
        else:
            init_date = period_of_interest[0].replace(day=1)

        start_hour = max(24, (period_of_interest[0] - init_date).days*24)
        end_hour = min(5160, (period_of_interest[1] - period_of_interest[0]).days*24 + start_hour)

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
                    utils.ceil_decimals(spi_utils._living_lab_bbox[living_lab]['max_y'], 0),    # N
                    utils.floor_decimals(spi_utils._living_lab_bbox[living_lab]['min_x'], 0),   # W
                    utils.floor_decimals(spi_utils._living_lab_bbox[living_lab]['min_y'], 0),   # S
                    utils.ceil_decimals(spi_utils._living_lab_bbox[living_lab]['max_x'], 0),    # E
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
        cds_poi_data = cds_poi_data.sortby(['time', 'lat', 'lon'])
        cds_poi_data = cds_poi_data.sel(time=(cds_poi_data.time.dt.date>=period_of_interest[0]) & (cds_poi_data.time.dt.date<=period_of_interest[1]))

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


    # Save data to collection
    def save_spi_coverage_to_collection(self, living_lab, spi_ts, periods_of_interest, month_spi_coverages):

        def build_data(spi_ts, periods_of_interest, month_spi_coverages):
            ds = xr.concat(month_spi_coverages, dim="time")
            ds = ds.assign_coords(time = [datetime.datetime.fromisoformat(p.isoformat()) for p in periods_of_interest])
            ds = ds.to_dataset()
            for r in ds.r.values.tolist():
                ds[f'spi{spi_ts}_r{r}'] = ds.sel(r=r).tp
            ds = ds.drop_dims(['r'])    
            return ds

        ds = build_data(spi_ts, periods_of_interest, month_spi_coverages)
        collection_params = spi_utils.create_s3_collection_data(living_lab, ds, data_type='forecast')
        spi_utils.update_config(living_lab, collection_params)
        return ds, collection_params
    
    
    def save_spi_basin_zonal_stats_to_collection(self, living_lab, spi_dataset, spi_coverage_collection_params):
        ds_zonal_stats = spi_utils.compute_zonal_stats(living_lab, spi_dataset)
        if ds_zonal_stats is not None:    
            collection_params = spi_utils.create_s3_zonal_stats_collection_data(living_lab, ds_zonal_stats, spi_coverage_collection_params, data_type='forecast')
            spi_utils.update_config(living_lab, collection_params)
            return ds_zonal_stats, collection_params
        else:
            LOGGER.debug('Zonal stats not computed due to missing basin geojson')
            return None
        


    def execute(self, data):

        mimetype = 'application/json'

        try:
            # Validate request params
            living_lab, periods_of_interest, spi_ts, out_format = spi_utils.validate_parameters(data, data_type='forecast')

            # Gather needed data (Ref + PoI)
            ref_dataset = spi_utils.read_ref_cds_data(living_lab)
            poi_dataset = self.query_poi_cds_data(living_lab, periods_of_interest, spi_ts)

            # Compute SPI coverage
            periods_of_interest, month_spi_coverages = self.compute_coverage_spi(ref_dataset, poi_dataset, spi_ts)

            # Save SPI coverage to collection
            ds, spi_coverage_collection_params = self.save_spi_coverage_to_collection(living_lab, spi_ts, periods_of_interest, month_spi_coverages)
            
            # Compute SPI basin-zonal-stats and save to collection
            zonal_stats_out = self.save_spi_basin_zonal_stats_to_collection(living_lab, ds, spi_coverage_collection_params)
            ds_zonal_stats, spi_zonal_stats_collection_params = zonal_stats_out if zonal_stats_out is not None else (None, None)
            if spi_zonal_stats_collection_params is not None:
                spi_zonal_stats_collection_info = {
                    'spi_zonal_stats_collection_id': spi_zonal_stats_collection_params['pygeoapi_id'],
                    'spi_zonal_stats_collection_s3_uri': spi_zonal_stats_collection_params['s3_uri'],
                }
            else:
                spi_zonal_stats_collection_info = {}

            # Convert SPI coverage in the requested output format
            output_data = {}
            if out_format is not None:
                out_spi_coverages = spi_utils.coverage_to_out_format(ds, out_format)
                output_data = {
                    'output_data': {
                        'format': out_format,
                        'collection_data': out_spi_coverages
                    }
                }
                if ds_zonal_stats is not None:
                    output_data['output_data']['zonal_stats'] = ds_zonal_stats.to_geo_dict()
            
            # Prepare response
            outputs = {
                'status' : 'OK',
                
                'spi_coverage_collection_id': spi_coverage_collection_params['pygeoapi_id'],
                'spi_coverage_collection_s3_uri': spi_coverage_collection_params['s3_uri'],
                
                ** spi_zonal_stats_collection_info,
                
                ** output_data
            }

        except spi_utils.Handle200Exception as err:
            outputs = {
                'status': err.status,
                'error': str(err)
            }
        except Exception as err:
            raise ProcessorExecuteError(str(err))

        return mimetype, outputs

    def __repr__(self):
        return f'<IngestorCDSSPIForecastProcessProcessor> {self.name}'
