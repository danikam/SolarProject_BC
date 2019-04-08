#!/usr/bin/env python3
import findspark
findspark.init()
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point, Polygon
import shapely.speedups
shapely.speedups.enable()   # Enable speedups of shapely procedures
import time
import os
import subprocess
import multiprocessing

# Initialize the spark context
sc=SparkContext()

# Get the number of CPUs on the given machine
N_CORES = multiprocessing.cpu_count()

# Get the path to the top level of the repo
with open(".PWD") as f:
  repo_path = f.readline().strip()

# Open the .shp file and select the BC data
fp = "%s/Tables/ContourData/gadm36_CAN_1.shp"%repo_path
data_can_broadcast = gpd.read_file(fp)
data_can=sc.broadcast(data_can_broadcast)

# Function to determine whether a particular point lies within BC
def is_in_bc(flnm_cntnt):
  filename=os.fsdecode(flnm_cntnt)
  lat = float(filename[12:17])
  long = float(filename[18:25])
  # Note: the '1' in loc[1, 'geometry'] is because BC is the second entry in the shapefile
  return data_can.value.loc[1, 'geometry'].contains(Point(long,lat))

# Function to save the input file to HDFS
def save_ir_file_2_hdfs(filename):
  lat = float(filename[12:17])
  long = float(filename[18:25])
  subprocess.call(["/opt/software/hadoop-2.8.5/bin/hdfs", "dfs", "-copyFromLocal", "%s/Tables/IrradianceData/%s"%(repo_path, filename), "/user/ubuntu/IrradianceData_isInBC/%.2f_%.2f.csv"%(lat, long)])
  return 1

start_time=time.time()

# Create the directory in HDFS to contain the filenames, if needed
subprocess.call(["/opt/software/hadoop-2.8.5/bin/hdfs", "dfs", "-mkdir", "-p", "/user/ubuntu/IrradianceData_isInBC"])

# Collect the filenames for the coordinates within BC
flnms_RDD=sc.textFile("file://%s/Tables/IrradianceData/filenames.txt"%repo_path).repartition(N_CORES).filter(is_in_bc).cache()
#print(flnms_RDD.take(1))

# Save each file whose coordinates lie within BC to HDFS
#print(flnms_RDD.count())
flnms_RDD.foreach(save_ir_file_2_hdfs)

print("Time elapsed: %ds"%(time.time()-start_time))


