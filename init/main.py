import os
import s3fs
import yaml


class InitialContainerCheck():
    def __init__(self, 
                 config_path: str,
                 s3OTC: dict) -> None:
        self.config_path = config_path
        self.s3OTC = s3OTC

        self.config = self.read_config()
        self.resources = self.extract_data_source()
        self.s3 = s3fs.S3FileSystem(endpoint_url = self.s3OTC['FSSPEC_S3_ENDPOINT_URL'],
                                    key = self.s3OTC['FSSPEC_S3_KEY'],
                                    secret = self.s3OTC['FSSPEC_S3_SECRET'],
                                    anon=False,
                                    client_kwargs={'config': None})

        self.config_out = self.create_new_config()

    def read_config(self):
        with open(self.config_path, 'r') as file:
            data = yaml.safe_load(file)
        return data

    def write_config(self):
        with open(self.config_path, 'w') as outfile:
            yaml.dump(self.config_out, outfile, default_flow_style=False)

    def extract_data_source(self):
        res = {}
        for resource in self.config['resources']:
            if 'providers' in self.config['resources'][resource]:
                if 'data' in self.config['resources'][resource]['providers'][0]:
                    res[resource] = self.config['resources'][resource]['providers'][0]['data']
        return(res)


    def create_new_config(self):
        new_config = self.config.copy()
        for dataset, data in self.config.items():
            if not self.s3.exists(data):
                new_config.pop(dataset)
        return new_config



if __name__ == '__main__':
    c = './config.yaml'

    # s3 = {'FSSPEC_S3_ENDPOINT_URL': 'https://obs.eu-de.otc.t-systems.com',
    #       'FSSPEC_S3_KEY': '',
    #       'FSSPEC_S3_SECRET': ''}

    s3 = {'FSSPEC_S3_ENDPOINT_URL': os.environ.get(key='FSSPEC_S3_ENDPOINT_URL'),
          'FSSPEC_S3_KEY': os.environ.get(key='FSSPEC_S3_KEY'),
          'FSSPEC_S3_SECRET': os.environ.get(key='FSSPEC_S3_SECRET')}

    checker = InitialContainerCheck(c, s3)
    config = checker.config
    print(checker.config_out)







        