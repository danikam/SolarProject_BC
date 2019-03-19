#!/usr/bin/env python3
# Date:     180318
# Purpose:   Calculate the maximum energy storage required for the 100 solar farm sites to satisfy energy needs without an external energy supply.

import findspark
findspark.init()
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
import numpy as np
import matplotlib.pyplot as plt
import time
import datetime as dt
from pytz import timezone
import multiprocessing
plt.rc('xtick', labelsize=20)
plt.rc('ytick', labelsize=20)


sc=SparkContext()
sql=SQLContext(sc)

start_time = time.time()

# Function to create zipped lists of latitude, longitude, and line content for each line in the input file
def make_data(fnm_content):
  
  # Extract the latitude and longitude from the filename
  fnm=fnm_content[0]
  basename = fnm.split("/")[-1]
  
  basename_underscore = basename.split("_")
  lat = float(basename_underscore[0])
  
  basename_period = basename_underscore[1].split(".")
  lon = float(basename_period[0] + "." + basename_period[1])
  
  # Make a list of latitudes and longitudes of the same length as the number of lines in the file
  content=fnm_content[1]
  list_of_lines = (content.split("\n"))[:-1]
  lats=[lat]*len(list_of_lines)
  lons=[lon]*len(list_of_lines)
  
  # Reformat the lines to display the info of interest
  reformatted_lines=[]
  for line in list_of_lines:
    line_list = line.split(",")
    year = int(line_list[0])
    month = int(line_list[2])
    day = int(line_list[3])
    hour = int(line_list[4])
    GHI = int(line_list[5])
    reformatted_lines.append((year, month, day, hour, GHI))
  
  # Zip together the latitude, longitude, and line contents
  return zip(lats,lons,reformatted_lines)

# Function to convert the year, month, day, and hour to a timestamp, and reformat the RDD line contents accordingly
def convert_data(line_ntuple):
  lat = line_ntuple[0]
  lon = line_ntuple[1]
  info = line_ntuple[2]
  year=info[0]
  month=info[1]
  day=info[2]
  hour=info[3]
  GHI=info[4]
  time_dt = dt.datetime(year, month, day, hour)
  time_utc = timezone('UTC').localize(time_dt)
  timestamp = time_utc.timestamp()
  return (lat, lon, year, timestamp, GHI)


# Get the number of CPUs on the given machine
N_CORES = multiprocessing.cpu_count()

# Get the path to the top level of the repo
with open(".PWD") as f:
  repo_path = f.readline().strip()

# Collect the load data for the years of interest
load_RDD = sc.textFile("%s/Tables/LoadData/BalancingAuthorityLoad*.csv")

def make_filename(coord_string):
  coord_list = coord_string.split(",")
  lat = coord_list[0]
  long = coord_list[1]
  return "/user/ubuntu/IrradianceData_isInBC/%s_%s.csv"%(lat, long)

# Collect the irradiance data for the selected coordinates
filenames = sc.textFile('/user/ubuntu/IrradianceMap/SampledCoords_df').map(make_filename).collect()
print(site_coords[0:5])

irr_RDD = sc.wholeTextFiles(','.join(filenames)).flatMap(make_data).repartition(2*N_CORES).map(convert_data)

print(irr_RDD.take(5))


