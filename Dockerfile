FROM gcr.io/world-fishing-827/github.com/globalfishingwatch/gfw-pipeline:latest

# Setup local application dependencies
COPY . /opt/project
RUN  apt-get update && \
  apt-get -qy install gdal-bin libgdal-dev && \
  export GDAL_VERSION="$(gdal-config --version)" && \
  pip install -r requirements-scheduler.txt && \
  pip install -e .

# Setup the entrypoint for quickly executing the pipelines
ENTRYPOINT ["scripts/run.sh"]
