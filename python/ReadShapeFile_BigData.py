#!/usr/bin/env python
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

sc=SparkContext()
sql=SQLContext(sc)

# Get the path to the top level of the repo
with open(".PWD") as f:
  repo_path = f.readline()

# Open the .shp file and select the BC data
fp = "ContourData/gadm36_CAN_1.shp"
data_can_broadcast = gpd.read_file(fp)
data_can=sc.broadcast(data_can_broadcast)

# Function to determine whether a particular point lies within BC
def isInBC(flnm_cntnt):
  filename=os.fsdecode(flnm_cntnt)
  lat = float(filename[12:17])
  long = float(filename[18:25])
  return data_can.value.loc[1, 'geometry'].contains(Point(long,lat))

# Function to copy the input file to a directory Tables/IrradianceData_isInBC
# This is temporary, because the hdfs isn't set up properly on the mac
# Long-term, things should be written to HDFS
def CopyIrFile(filename):
  lat = float(filename[12:17])
  long = float(filename[18:25])
  copyfile("%s/Tables/IrradianceData/%s"%(repo_path, filename), "%s/Tables/IrradianceData_isInBC/%.2f_%.2f.csv"%(repo_path, lat, long))
  return 1

start_time=time.time()

# Collect the filenames for the coordinates within BC
flnms_RDD=sc.textFile("file://%s/Tables/list.txt"%repo_path).repartition(4).filter(isInBC).cache()
#print(flnms_RDD.take(1))

# Copy each file whose coordinates lie within BC to a dedicated directory
flnms_RDD.foreach(CopyIrFile)

# Collect the coordinates within BC (lat, long)
#coords_RDD=flnms_RDD.map(lambda x: (float(x[12:17]), float(x[18:25])))

#print(coords_RDD.take(1))

#irr_RDD=flnms_RDD.map(lambda x: np.genfromtxt("/Users/danikamacdonell/Courses/Phys511A/SolarProject/Tables/IrradianceData_BC/%s"%x, delimiter=","))

#print(flnms_RDD.collect())
#irr_RDD=sc.textFile(",".join(["Tables/IrradianceData_BC/"+s for s in flnms_RDD.collect()]))

#print(irr_RDD.take(1))

print("Time elapsed: %ds"%(time.time()-start_time))


