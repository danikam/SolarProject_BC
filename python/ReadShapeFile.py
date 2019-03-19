#!/usr/bin/env python3
import findspark
findspark.init()
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
import pandas as pd
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point, Polygon
import shapely.speedups
shapely.speedups.enable()   # Enable speedups of shapely procedures
import time
from shutil import copyfile
import os
import subprocess
import multiprocessing

sc=SparkContext()
sql=SQLContext(sc)

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
def isInBC(flnm_cntnt):
  filename=os.fsdecode(flnm_cntnt)
  lat = float(filename[12:17])
  long = float(filename[18:25])
  return data_can.value.loc[1, 'geometry'].contains(Point(long,lat))
#  return 1

# Function to copy the input file to a directory Tables/IrradianceData_isInBC
def CopyIrFile(filename):
  lat = float(filename[12:17])
  long = float(filename[18:25])
  copyfile("%s/Tables/IrradianceData/%s"%(repo_path, filename), "%s/Tables/IrradianceData_isInBC/%.2f_%.2f.csv"%(repo_path, lat, long))
  return 1

# Function to save the input file to HDFS
def SaveIrFile2HDFS(filename):
  lat = float(filename[12:17])
  long = float(filename[18:25])
  subprocess.call(["/opt/software/hadoop-2.8.5/bin/hadoop", "fs", "-copyFromLocal", "%s/Tables/IrradianceData/%s"%(repo_path, filename), "/user/ubuntu/IrradianceData_isInBC/%.2f_%.2f.csv"%(lat, long)])
  return 1

start_time=time.time()

# Collect the filenames for the coordinates within BC
flnms_RDD=sc.textFile("file://%s/Tables/IrradianceData/filenames.txt"%repo_path).repartition(N_CORES).filter(isInBC).cache()
#print(flnms_RDD.take(1))

# Copy each file whose coordinates lie within BC to a dedicated directory
#flnms_RDD.foreach(CopyIrFile)

# Save each file whose coordinates lie within BC to HDFS
#print(flnms_RDD.count())
flnms_RDD.foreach(SaveIrFile2HDFS)

print("Time elapsed: %ds"%(time.time()-start_time))


