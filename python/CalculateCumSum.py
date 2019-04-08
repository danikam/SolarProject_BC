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
import pyarrow as pa
plt.rc('xtick', labelsize=20)
plt.rc('ytick', labelsize=20)

sc=SparkContext()
sql=SQLContext(sc)

# Get the number of CPUs on the given machine
N_CORES = multiprocessing.cpu_count()

# Get the path to the top level of the repo
with open(".PWD") as f:
  repo_path = f.readline().strip()
  
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
  timestamp = int(time_utc.timestamp())
  return (lat, lon, year, timestamp, GHI)

def reformat_load_data(line_content):
  content_list = line_content.split(",")
  date_list = content_list[0].split("/")
  year = int(date_list[0])
  month = int(date_list[1])
  day = int(date_list[2])
  hour = int(content_list[1])-1

  # Convert the date and hour to a timestamp
  time_dt = dt.datetime(year, month, day, hour)
  time_pacific = timezone('US/Pacific').localize(time_dt)
  timestamp = int(time_pacific.timestamp())

  # Save the reformatted data as an ntuple
  load = int(content_list[2])    # Load, in MWh
  return (year, timestamp, load)

# Collect the load data for the years of interest
load_RDD = sc.textFile("file://%s/Tables/LoadData/BalancingAuthorityLoad*.csv"%repo_path).filter(lambda x: x[0]!="#").map(reformat_load_data).repartition(2*N_CORES)

# Convert to a dataframe
load_DF = load_RDD.toDF(["Year", "Load_Timestamp", "Load"])
load_DF.show(n=5)

# Function to convert a string of coordinates into the hdfs filename containing irradiance data for the given set of coordinates
def make_filename(coord_string):
  coord_list = coord_string.split(",")
  lat = coord_list[0]
  long = coord_list[1]
  return "/user/ubuntu/IrradianceData_isInBC/%s_%s.csv"%(lat, long)

# Collect the irradiance data for the selected coordinates
filenames = sc.textFile('/user/ubuntu/IrradianceMap/SampledCoords_df').map(make_filename).collect()
irr_RDD = sc.wholeTextFiles(','.join(filenames)).flatMap(make_data).repartition(2*N_CORES).map(convert_data)

# Convert to a dataframe, and sum the GHI over the latitudes and longitudes
irr_DF = irr_RDD.toDF(["Lat", "Long", "Year", "Irr_Timestamp", "GHI"]).groupBy("Year", "Irr_Timestamp").sum("GHI").withColumnRenamed("sum(GHI)", "GHI_Sum")
irr_DF.show(n=5)

# Join the dataframes so the timestamps are matching. 
power_DF = irr_DF.join(load_DF.drop("Year"), load_DF.Load_Timestamp==irr_DF.Irr_Timestamp).drop("Load_Timestamp").withColumnRenamed("Irr_Timestamp", "Timestamp").repartition(2*N_CORES).cache()

# Filter out any rows with 0 load 
power_DF = power_DF.filter(power_DF.Load > 0)
power_DF.show(n=24)

# Find the cumulative sum of the load and GHI columns
power_DF = power_DF.selectExpr(
    "Year", "Timestamp", "GHI_Sum", "Load",  
    "sum(GHI_sum) over (order by Timestamp) as GHI_cumsum", "sum(Load) over (order by Timestamp) as Load_cumsum").repartition(2*N_CORES).sort("Timestamp").cache()
power_DF.show(n=24)

# Normalize the GHI sum and cumulative sum so that the final cumulative sum of the GHI is the same as that for the load
maxTimestamp = power_DF.groupby().max('Timestamp').collect()[0][0]
lastRow = power_DF.filter(power_DF.Timestamp == maxTimestamp).rdd.map(tuple).collect()[0]
#print(lastRow)
maxGHI = lastRow[4]
maxLoad = lastRow[5]
powerRatio = maxLoad/maxGHI
#print(maxGHI, maxLoad)

# Multiply the GHI columns by the scaling factor, and compute the difference between the scaled cumulative GHI and the cumulative load
power_RDD = power_DF.rdd.map(tuple).map(lambda x: (x[0], x[1], x[2], x[2]*powerRatio, x[3], x[4], x[4]*powerRatio, x[5])).repartition(2*N_CORES).cache()
power_RDD = power_RDD.map(lambda x: (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[6]-x[7]))
#max_energy_list = power_RDD.max(lambda x: abs(x[6]))
#print(max_energy_list)

# Delete the directory to write to if needed
fs = pa.hdfs.connect(host="bgdtn", port=9000, user="ubuntu")
try:
  fs.delete('/user/ubuntu/SolarAnalysis/PowerCumSum', recursive=True)
  print("Removed HDFS directory /user/ubuntu/SolarAnalysis/PowerCumSum")
except IOError:
  print("HDFS directory /user/ubuntu/SolarAnalysis/PowerCumSum not present - no need to delete. (Or some other nefarious IO error...)")

# Save the RDD to text files
power_RDD.map(lambda x: ','.join(str(d) for d in x)).saveAsTextFile('/user/ubuntu/SolarAnalysis/PowerCumSum')
 
print("Elapsed Time: %ds"%(time.time()-start_time))
