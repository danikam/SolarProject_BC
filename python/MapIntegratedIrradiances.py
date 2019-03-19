#!/usr/bin/env python3
# Date:     180312                                                                                                                                                                                                                            
# Purpose:   Calculate the integrated solar irradiance for all locations in BC over all years from 2002 to 2008, and visualize as a 2D colour plot. Use the integrated solar irradiance to randomly select solar farm sites, with a preference for higher-irradiance areas.

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

sc=SparkContext()
sql=SQLContext(sc)

start_time = time.time()

# Get the number of CPUs on the given machine
N_CORES = multiprocessing.cpu_count()

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

# Open the irradiance data as whole text files, and reformat into ntuples (lat, long, year, timestamp, GHI))
irr_RDD=sc.wholeTextFiles('/user/ubuntu/IrradianceData_isInBC/*.csv').flatMap(make_data).repartition(2*N_CORES).map(convert_data)
#print(irr_RDD.take(1))  
  
# Convert the RDD to a dataframe
irr_DF = irr_RDD.toDF(["Lat", "Long", "Year", "Timestamp", "GHI"])
#irr_DF.show(n=2)

# Save the full irradiance dataframe to HDFS
irr_DF.write.save('/user/ubuntu/IrradianceMap/TimeSeriesIrradiance_df', format='csv', mode='overwrite')

# Obtain the average irradiance for each latitude and longitude
irr_avg_DF = irr_DF.groupby("Lat", "Long").avg("GHI")
#irr_avg_DF.show(n=10)

# Save the average irradiance dataframe to HDFS
irr_avg_DF.write.save('/user/ubuntu/IrradianceMap/AverageIrradiance_df', format='csv', mode='overwrite')

print("Time elapsed: %ds"%(time.time()-start_time))
