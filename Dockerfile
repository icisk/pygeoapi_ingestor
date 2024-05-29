FROM geopython/pygeoapi:latest

ENV PYGEOAPI_CONFIG=config.yml

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git && \
    apt-get install -y cron && \
    apt-get install -y curl

COPY ./plugins/ ./plugins
RUN pip install ./plugins netcdf4 h5netcdf shyaml python-dotenv cdsapi ecmwflibs cfgrib
RUN rm -rf ./process

COPY ./entrypoint.sh /entrypoint.sh
COPY ./scheduler.sh /scheduler.sh
COPY ./invoke_smhi_ingestor.py /invoke_smhi_ingestor.py
COPY ./invoke_cds_ingestor.py /invoke_cds_ingestor.py

ENTRYPOINT ["/entrypoint.sh"]
