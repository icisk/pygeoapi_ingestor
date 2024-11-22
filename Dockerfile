FROM geopython/pygeoapi:latest

ENV PYGEOAPI_CONFIG=config.yml

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y \
        curl \
        git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /pygeoapi

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
COPY ./provider/geojson.py /pygeoapi/pygeoapi/provider/geojson.py

RUN pip install . \
        # cdsapi \
        # h5netcdf \
        # netcdf4 \
        # python-dotenv \
        # shyaml \
        # geojson \
        # geopandas \
        # numpy==1.26.4 \
    && rm -rf ./process


# entrypoint.sh and scheduler.sh EOL must be UNIX-style (LF). If not you can occur in the following error: exec /entrypoint.sh: no such file or directory
RUN chmod +x /pygeoapi/entrypoint.sh

RUN mkdir -p /pygeoapi/config

ENTRYPOINT ["/pygeoapi/entrypoint.sh", "run"]
