#!/usr/bin/env python3
import findspark
findspark.init()
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
import numpy as np
import matplotlib.pyplot as plt
import time
from shutil import copyfile
import os
import io
import glob

sc=SparkContext()
sql=SQLContext(sc)

# Get the path to the top level of the repo
with open(".PWD") as f:
  repo_path = f.readline().strip()

# Function to make a triplet (lat, long, line) for each line in a given file
# The latitude and longitude are parsed from the filename f
def proc(f):
  return sc.textFile(f).map(lambda x: (float((f.split("/")[-1])[0:5]), float((f.split("/")[-1])[6:13]), x))

# Create an RDD from the union of triplets obtained from each irradiance file
irr_RDD = sc.union([proc(f) for f in glob.glob("%s/Tables/IrradianceData_isInBC/*.csv"%repo_path)])

# Convert each time in a line to a year and timestamp

print(irr_RDD.take(1))

# Read in the irradiance files as whole text files, and pair them with their coordinates
#flnms_RDD=sc.textFile('file:///Users/danikamacdonell/Courses/Phys511A/SolarProject/Tables/IrradianceData_isInBC/list.txt')

#coords_RDD=flnms_RDD.map(lambda x: (x.split("_")[0], (x.split("_")[1])[0:7]))
  
#irr_RDD = sc.textFile("/Users/danikamacdonell/Courses/Phys511A/SolarProject/Tables/IrradianceData_isInBC/*.csv")
#print(irr_RDD).inputFiles()
  #irr_RDD = sc.wholeTextFiles("/Users/danikamacdonell/Courses/Phys511A/SolarProject/Tables/IrradianceData_isInBC/*.csv").\
#  map(lambda x: np.genfromtxt(io.StringIO(x[1]), delimiter=",")[:,[0,2,3,4,5]])

#paired_RDD = coords_RDD.zip(irr_RDD)



              #print(paired_RDD.take(1))
