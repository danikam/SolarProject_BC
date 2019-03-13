#!/usr/bin/env python3
# Date:     180312                                                                                                                                                                                                                            
# Purpose:   Calculate the integrated solar irradiance for all locations in BC over all years from 2002 to 2008, and visualize as a 2D colour plot. Normalize the integrated irradiances to make a probability distribution for locating the solar farms.  

import findspark
findspark.init()
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import Window
from pyspark.sql import functions as F
import numpy as np
import matplotlib.pyplot as plt
import time
from shutil import copyfile
import os
import io
import glob
#from hdfs import InsecureClient
import subprocess
import datetime as dt
from pytz import timezone

sc=SparkContext()
sql=SQLContext(sc)

start_time = time.time()

# Get the path to the top level of the repo
with open(".PWD") as f:
  repo_path = f.readline().strip()
  
# Function to make an ntuple (lat, long, year, time stamp, GHI) for each line in a given file
# The latitude and longitude are parsed from the filename f
def proc(f):
  
  # Function to make the data line for the given file f
  def MakeDataLine(line):
    line_list = line.split(',')
    year = int(line_list[0])
    month = int(line_list[2])
    day = int(line_list[3])
    hour = int(line_list[4])
    GHI = int(line_list[5])
    
    # Convert the year, month, day, and hour (in UTC) to a timestamp
    time_dt = dt.datetime(year, month, day, hour)
    time_utc = timezone('UTC').localize(time_dt)
    timestamp = time_utc.timestamp()
    return (float((f.split("/")[-1])[0:5]), float((f.split("/")[-1])[6:13]), year, timestamp, GHI)

  return sc.textFile(f).map(MakeDataLine)

# Get a list of all files in the IrradianceData_isInBC directory in HDFS
p=subprocess.Popen(["/opt/software/hadoop-2.8.5/bin/hadoop", "fs", "-ls", "-C", "/user/ubuntu/IrradianceData_isInBC/*.csv"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
out, err = p.communicate()
filelist = out.decode("utf-8").split('\n')[:-1]

# Create an RDD from the union of ordered ntuples obtained from each irradiance file
#irr_RDD = sc.union([proc(f) for f in glob.glob("file://%s/Tables/IrradianceData_isInBC/*.csv"%repo_path)])
irr_RDD = sc.union([proc(f) for f in filelist])  
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
