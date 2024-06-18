FROM geopython/pygeoapi:latest

ENV PYGEOAPI_CONFIG=config.yml

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y \
        cron \
        curl \
        git \
    && rm -rf /var/lib/apt/lists/*

COPY ./plugins/ ./plugins

RUN pip install \
        ./plugins \
        cdsapi \
        h5netcdf \
        netcdf4 \
        python-dotenv \
        shyaml \
    && rm -rf ./process

COPY ./entrypoint.sh /entrypoint.sh
COPY ./scheduler.sh /scheduler.sh
COPY ./invoke_smhi_ingestor.py /invoke_smhi_ingestor.py
COPY ./invoke_cds_ingestor.py /invoke_cds_ingestor.py
COPY ./invoke_planetary_ingestor.py /invoke_planetary_ingestor.py

# entrypoint.sh and scheduler.sh EOL must be UNIX-style (LF). If not you can occur in the following error: exec /entrypoint.sh: no such file or directory
RUN chmod +x /entrypoint.sh \
    && chmod +x /scheduler.sh

ENTRYPOINT ["/entrypoint.sh"]
