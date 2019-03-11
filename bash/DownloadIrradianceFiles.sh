#!/bin/bash

# Date:     190311
# Purpose:  Copy the irradiance files of interest (longitudes that may lie within BC) from the database ftp://ftp.nrcan.gc.ca

# Get the path to the top-level directory of the repo
repo_dir=`cat .PWD`

# cd into the folder that the data will be downloaded to
cd ${repo_dir}/Tables/IrradianceData

# Loop through the longitude coordinates of interest, downloading and unzipping the zip file for each
for longitude in {114..129}
do
  wget ftp://ftp.nrcan.gc.ca/energy/SOLAR/WesternCanada_2002-2008/TimeSeriesByLatLon/*$longitude*.zip
  unzip "*${longitude}*.zip"
done
