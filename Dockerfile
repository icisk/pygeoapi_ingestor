FROM geopython/pygeoapi:latest

ENV PYGEOAPI_CONFIG=config.yml

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git 

COPY ./plugins/ ./plugins
RUN pip install ./plugins netcdf4 h5netcdf shyaml
RUN rm -rf ./process

COPY ./entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
