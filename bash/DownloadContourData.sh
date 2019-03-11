#!/bin/bash

# Date      190311
# Purpose:  Download the .shp file containing contour data for Canadian provincial borders to Tables/ContourData

# Get the path to the top-level directory of the repo
repo_dir=`cat .PWD`

# cd into the folder that the data will be downloaded to
cd ${repo_dir}/Tables/ContourData

# Download and unzip the file
wget https://biogeo.ucdavis.edu/data/gadm3.6/shp/gadm36_CAN_shp.zip
unzip gadm36_CAN_shp.zip
