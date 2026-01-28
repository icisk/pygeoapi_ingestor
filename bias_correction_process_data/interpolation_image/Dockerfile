FROM rocker/r-ver:4.3.1

RUN apt-get update && apt-get install -y \
    cmake \
    build-essential \
    libabsl-dev \
    libcurl4-gnutls-dev \
    libssl-dev \
    libxml2-dev \
    libudunits2-dev \
    libgdal-dev \
    libgeos-dev \
    libproj-dev \
    gdal-bin \
    libnetcdf-dev \
    s3cmd \
    vim \
    && rm -rf /var/lib/apt/lists/*

#grundgütiger
#RUN R -e "install.packages(c('ncdf4','lubridate','dplyr','tidyr','gstat','stars','sf','jsonlite','httr','glue'), dependencies=TRUE, repos='https://cloud.r-project.org/')"

RUN R -e "install.packages( \
  c('ncdf4','lubridate','dplyr','tidyr','gstat','stars','sf','jsonlite','httr','glue', 'zip', 'paws.storage'), \
  dependencies = c('Depends','Imports','LinkingTo'), \
  repos = 'https://cloud.r-project.org/' \
)"


ENV META_URL=https://52n-i-cisk.obs.eu-de.otc.t-systems.com/data-ingestor/spain/bias_correction/metadata.json
ENV COL_ENDPOINT=https://i-cisk.dev.52north.org/data/collections/

ENV FSSPEC_S3_ENDPOINT_URL=https://obs.eu-de.otc.t-systems.com
ENV DEFAULT_BUCKET=52n-i-cisk

#ENV DS_TO_PROCESS=2025-09
WORKDIR /app
RUN mkdir zip
COPY . .


ENTRYPOINT ["Rscript", "main.R"]
#ENTRYPOINT ["sleep", "infinity"]