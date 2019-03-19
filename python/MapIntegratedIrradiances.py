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
  GHIs = []
  for line in list_of_lines:
    GHI = int(line_list[5])
    GHIs.append(GHI)
  
  # Zip together the latitude, longitude, and GHI
  return zip(lats, lons, GHIs)

# Open the irradiance data as whole text files, and reformat into ntuples (lat, long, year, timestamp, GHI))
irr_RDD = sc.wholeTextFiles('/user/ubuntu/IrradianceData_isInBC/*.csv').flatMap(make_data).repartition(2*N_CORES)
#print(irr_RDD.take(1))  
  
# Convert the RDD to a dataframe
irr_DF = irr_RDD.toDF(["Lat", "Long", "GHI"])
#irr_DF.show(n=2)

# Obtain the average irradiance for each latitude and longitude
irr_avg_DF = irr_DF.groupby("Lat", "Long").avg("GHI")
#irr_avg_DF.show(n=10)

# Save the average irradiance dataframe to HDFS
irr_avg_DF.write.save('/user/ubuntu/IrradianceMap/AverageIrradiance_df', format='csv', mode='overwrite')

print("Time elapsed: %ds"%(time.time()-start_time))
