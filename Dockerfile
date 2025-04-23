FROM geopython/pygeoapi:0.20.0

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
RUN pip install git+https://github.com/icisk/IBF-drought-pipeline@dev --no-cache
# entrypoint.sh and scheduler.sh EOL must be UNIX-style (LF). If not you can occur in the following error: exec /entrypoint.sh: no such file or directory
RUN chmod +x /pygeoapi/entrypoint.sh

RUN mkdir -p /pygeoapi/config

ENTRYPOINT ["/pygeoapi/entrypoint.sh", "run"]

ARG GIT_HASH=commit-undefined
LABEL org.opencontainers.image.revision="${GIT_HASH}"

ARG BUILD_DATE=build-date-undefined
LABEL org.opencontainers.image.created="${BUILD_DATE}"

# Add build info to deployed version available via pygeoapi-context-path/static/info.txt
ARG INFO_FILE=pygeoapi/static/info.txt
ARG GIT_BRANCH=branch-undefined
ARG GIT_TAG=tag-undefined
RUN touch "${INFO_FILE}" \
 && echo "Build" > "$INFO_FILE" \
 && echo "-----" >> "$INFO_FILE" \
 && echo "timestamp: $(date)" >> "$INFO_FILE" \
 && echo "git hash: $(echo $GIT_HASH | cut -c1-20)" >> "$INFO_FILE" \
 && echo "git branch: $GIT_BRANCH" >> "$INFO_FILE" \
 && echo "git tag: $GIT_TAG" >> "$INFO_FILE" \
 && echo "pygeoapi: $(pygeoapi --version)" >> "$INFO_FILE" \
 && cat "${INFO_FILE}"

RUN sed -i '/{{ version }}/a \
 \(<a title="info" id="showInfo" href="{{ config["server"]["url"] }}/static/info.txt">info</a>\)\
 <script>\
 document.getElementById("showInfo").addEventListener("click", function(event) {\
   event.preventDefault();\
   fetch("{{ config["server"]["url"] }}/static/info.txt")\
     .then(response => response.text())\
     .then(data => {\
       alert(data);\
     })\
     .catch(error => {\
       alert("Error loading the file: " + error);\
     });\
 });\
 </script>' /pygeoapi/pygeoapi/templates/_base.html
