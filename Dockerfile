FROM geopython/pygeoapi:latest

ENV PYGEOAPI_CONFIG=config.k8s.yml
ENV WSGI_WORKER_TIMEOUT=289067349086745908673459

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y \
        curl \
        git \
        gdal-bin\
        vim \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /pygeoapi

COPY . .

RUN pip install . \
    && rm -rf ./process

RUN pip install git+https://github.com/icisk/pygeoapi-provider-s3-geojson.git
# entrypoint.sh and scheduler.sh EOL must be UNIX-style (LF). If not you can occur in the following error: exec /entrypoint.sh: no such file or directory
RUN chmod +x /pygeoapi/entrypoint.sh

RUN mkdir -p /pygeoapi/config

ENTRYPOINT ["/pygeoapi/entrypoint.sh", "run"]

# Add build info to deployed version available via pygeoapi-context-path/static/info.txt
ARG INFO_FILE=pygeoapi/static/info.txt
ARG GIT_HASH=hash-undefined
ARG GIT_BRANCH=branch-undefined
ARG GIT_TAG=tag-undefined
RUN touch "${INFO_FILE}" \
 && echo "Build" > "$INFO_FILE" \
 && echo "----------------------------------------------------" >> "$INFO_FILE" \
 && echo "timestamp  : $(date)" >> "$INFO_FILE" \
 && echo "git hash   : $GIT_HASH" >> "$INFO_FILE" \
 && echo "git branch : $GIT_BRANCH" >> "$INFO_FILE" \
 && echo "git branch : $GIT_TAG" >> "$INFO_FILE" \
 && cat "${INFO_FILE}"
