import logging
import os

import geopandas as gpd
import yaml
from dotenv import load_dotenv, find_dotenv
from filelock import FileLock
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
from sqlalchemy import create_engine

LOGGER = logging.getLogger(__name__)

load_dotenv(find_dotenv())

PROCESS_METADATA = {
    'version': '0.2.0',
    'id': 'maps_api_ingestor',
    'title': {
        'en': 'maps_api',
    },
    'description': {
        'en': 'creates maps from layers in postgis db'},
    'jobControlOptions': ['sync-execute', 'async-execute'],
    'keywords': ['ingestor process'],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'information',
        'href': 'https://example.org/process',
        'hreflang': 'en-US'
    }],
    'inputs': {
        'layer_name': {
            'title': 'layer_name',
            'description': 'the layer (table) name in the postgis db',
            'schema': {
                'type': 'string'
            }
        },
        'sld': {
            'title': 'sld_path',
            'description': 'path to sld file (/pygeoapi/secondary_process_data/sld/....)',
            'schema': {
                'type': 'string'
            }
        },
        'token': {
            'title': 'secret token',
            'description': 'identify yourself',
            'schema': {
                'type': 'string'
            }
        }
    },
    'outputs': {
        'id': {
            'title': 'ID',
            'description': 'The ID of the process execution',
            'schema': {
                'type': 'string'
            }
        },
        'value': {
            'title': 'Value',
            'description': 'status msg describing the process',
            'schema': {
                'type': 'string'
            }
        }
    },
    'example': {
        "inputs": {
            "layer_name": "ll_spain_example_layer",
            "sld": "pygeoapi/secondary_process_data/sld/example.sld",
            "token": "ABC123XYZ666"
        }
    }
}

class IngestorAPIMAPSProcessProcessor(BaseProcessor):
    """
    Ingestor Processor

    puts gpkg in db

    Resource Requirements:
    - CPU: 3
    - RAM: 12G
    """

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition

        :returns: pygeoapi.process.ingestor_process.IngestorLLGPKGProcessProcessor
        """

        super().__init__(processor_def, PROCESS_METADATA)
        self.config_file = os.environ.get(default='/pygeoapi/serv-config/local.config.yml', key='PYGEOAPI_SERV_CONFIG')
        self.title = None
        self.db_user = os.environ.get(key='DB_USER', default="postgres")
        self.db_password = os.environ.get(key='DB_PASSWORD',default= "password")
        self.db_host = os.environ.get(key='DB_HOST', default="localhost")
        self.db_port = int(os.environ.get(key='DB_PORT', default="5432"))
        self.db_database = os.environ.get(key='DB_DATABASE', default="postgres")
        self.data = None
        self.data_geom_type = None
        self.shp_path = None
        self.shp_base_path = None
        self.sld_path = None
        self.layer_name = None
        self.maps_geom_type = None


    def read_config(self):
        with open(self.config_file, 'r') as file:
            LOGGER.debug("read config")
            return(yaml.safe_load(file))

    def write_config(self, new_config):
        with  open(self.config_file, 'w') as outfile:
            yaml.dump(new_config, outfile, default_flow_style=False)
        LOGGER.debug("updated config")

    def get_geomtype(self):
        if self.data_geom_type == "Polygon" or self.data_geom_type == "MultiPolygon":
            return "MS_LAYER_POLYGON"
        if self.data_geom_type == "Point" or self.data_geom_type == "MultiPoint":
            return "MS_LAYER_POINT"
        if self.data_geom_type == "LineString" or self.data_geom_type == "MultiLineString":
            return "MS_LAYER_LINE"
        return None


    def update_config(self):
        min_x, min_y, max_x, max_y = [float(val) for val in self.data.total_bounds]


        # THIS MUST BE THE SAME IN ALL PROCESSES UPDATING THE SERV CONFIG
        lock = FileLock(f"{self.config_file}.lock")

        with lock:

            config= self.read_config()
            config['resources'][f'maps_api_{self.layer_name}'] = {
                'type': 'collection',
                'title': f'maps_api_{self.layer_name}',
                'description': f'maps api of {self.layer_name} for',
                'keywords': ['country'],
                'extents': {
                    'spatial': {
                        'bbox': [min_x, min_y, max_x, max_y],
                        'crs': 'http://www.opengis.net/def/crs/EPSG/0/4326'
                    },
                },
                'providers':
                    [{
                        'type': 'map',
                        'name': 'MapScript',
                        'options': {
                            'type': self.maps_geom_type,
                            'layer': self.layer_name,
                            'style': self.sld_path
                        },
                        'data': f'{self.shp_path}',
                        'format': {
                            "name": "png",
                            "mimetype": "image/png"
                        }
                    }]
            }

            self.write_config(config)


    def check_config_if_ds_is_collection(self):
        config = self.read_config()
        return self.title in config['resources']


    def execute(self, data):
        mimetype = 'application/json'

        self.layer_name = data.get('layer_name')
        self.sld_path = data.get("sld")
        self.token = data.get('token')

        LOGGER.debug(f"checking process inputs")
        if self.layer_name is None:
            raise ProcessorExecuteError('Cannot process without a layer name')
        if self.token is None:
            raise ProcessorExecuteError('Identify yourself with valid token!')
        if self.sld_path is None:
            raise ProcessorExecuteError('No lsd specified')

        LOGGER.debug(f"checking token")
        if self.token != os.getenv("INT_API_TOKEN", "token"):
            #FIXME passender error?
            LOGGER.error("WRONG INTERNAL API TOKEN")
            raise ProcessorExecuteError('ACCESS DENIED wrong token')


        engine = create_engine(f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_database}")
        query = f"SELECT * FROM {self.layer_name};"
        self.data = gpd.read_postgis(query, engine, geom_col='geometry')
        
        self.data_geom_type = self.data.geom_type.unique()
        self.maps_geom_type = self.get_geomtype()
        if self.maps_geom_type is None:
            raise ProcessorExecuteError('cant use geometry type to create api maps')

        self.shp_base_path = f"/pygeoapi/data_share/shp/{self.layer_name}"
        self.shp_path = os.path.join(self.shp_base_path, f'{self.layer_name}.shp')
        os.makedirs(self.shp_base_path, exist_ok=True)

        self.data.to_file(self.shp_path, driver="ESRI Shapefile")

        self.update_config()


        outputs = {
            'id': 'api_maps',
            'value': 'done'
        }

        return mimetype, outputs

    def __repr__(self):
        return f'<IngestorAPIMAPSProcessProcessor> {self.name}'