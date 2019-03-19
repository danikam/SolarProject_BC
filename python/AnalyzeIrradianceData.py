#!/usr/bin/env python3
import findspark
findspark.init()
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as f
import matplotlib.pyplot as plt
from matplotlib import cm
import time
import numpy as np
from pyspark.sql.functions import lit
import subprocess

sc=SparkContext()
sql=SQLContext(sc)

start_time = time.time()

# Read in the RDD from HDFS
power_RDD = textFile('/user/ubuntu/SolarAnalysis/PowerCumSum').cache()

# Shift the cumulative GHI upwards so that it's always at least as large as the load
min_diff_list = power_RDD.min(lambda x: x[6])
min_diff = min_diff_list[6]

# If the minimum difference is negative, shift the GHIs upwards so that the minimum difference becomes 0. Otherwise, do nothing.
if min_diff < 0:
  print("Shifting the cumulative GHIs upwards by %d MWh"%(-min_diff))
  power_RDD = power_RDD.map(lambda x: (x[0], x[1], x[2], x[3], x[4]-min_diff, x[5], x[6]))

# Calculate the maximum difference between the cumulative GHI and the cumulative load
max_diff_list = power_RDD.max(lambda x: x[6]).collect()
max_diff = max_energy_list[6]

print("Maximum storage capacity needed: %dMWh"%(max_diff))

print("Time Elapsed: %ds"%(time.time()-start_time))
