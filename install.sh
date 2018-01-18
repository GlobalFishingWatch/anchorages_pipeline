#!/bin/bash

# This script is designed to be called from outside the  docker container
# with an install dir mounted at /dags
#
# See https://github.com/GlobalFishingWatch/docker-airflow  install_dags.sh

echo ""
echo "Installing files..."

cp -Rv ./airflow/* /dags | sed 's/^/     /'

echo "Installation Complete"