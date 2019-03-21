#!/usr/bin/env python3
import findspark
findspark.init()
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as f
import matplotlib.pyplot as plt
from matplotlib import cm
from matplotlib.gridspec import GridSpec
import time
import numpy as np
import datetime as dt
from pyspark.sql.functions import lit
import subprocess
import multiprocessing
plt.rc('xtick', labelsize=20)
plt.rc('ytick', labelsize=20)

sc=SparkContext()
sql=SQLContext(sc)

start_time = time.time()

# Get the number of CPUs on the given machine
N_CORES = multiprocessing.cpu_count()

# Define constants to keep track of the indices of the power_RDD ntuple
iYEAR=0
iTIMESTAMP=1
iGHI=2
iGHI_SCALED=3
iLOAD=4
iGHI_CUMSUM=5
iGHI_CUMSUM_SCALED=6
iLOAD_CUMSUM=7
iDIFF=8

def create_ntuple(line):
  line_list = line.split(",")
  return (int(line_list[iYEAR]), int(line_list[iTIMESTAMP]), int(line_list[iGHI]), float(line_list[iGHI_SCALED]), int(line_list[iLOAD]), int(line_list[iGHI_CUMSUM]), float(line_list[iGHI_CUMSUM_SCALED]), int(line_list[iLOAD_CUMSUM]), float(line_list[iDIFF]))

# Read in the RDD from HDFS
#power_RDD = sql.read.format('csv').load("/user/ubuntu/SolarAnalysis/PowerCumSum", inferSchema=True).withColumnRenamed("_c1", "Timestamp").sort("Timestamp").rdd.map(tuple).repartition(2*N_CORES).cache()
power_RDD = sc.textFile('/user/ubuntu/SolarAnalysis/PowerCumSum/*').map(create_ntuple).repartition(2*N_CORES).cache()
print(power_RDD.take(3))

# Shift the cumulative GHI upwards so that it's always at least as large as the load
min_diff_list = power_RDD.min(lambda x: x[8])
min_diff = min_diff_list[8]

# If the minimum difference is negative, shift the GHIs upwards so that the minimum difference becomes 0. Otherwise, do nothing.
if min_diff < 0:
  print("Shifting the cumulative GHIs upwards by %d MWh"%(-min_diff))
  power_RDD = power_RDD.map(lambda x: (x[0], x[1], x[2], x[3], x[4], x[5], x[6]-min_diff, x[7], x[8]-min_diff)).repartition(2*N_CORES).sortBy(lambda x: x[iTIMESTAMP]).cache()

# Calculate the maximum difference between the cumulative GHI and the cumulative load
max_diff_list = power_RDD.max(lambda x: x[8])
max_diff = max_diff_list[6]

print("Maximum storage capacity needed: %dTWh"%(max_diff/1e6))

# Plot the time variation in raw GHI and load
times = power_RDD.map(lambda x: dt.datetime.fromtimestamp(x[iTIMESTAMP])).collect()
loads = power_RDD.map(lambda x: x[iLOAD]).collect()
fig = plt.figure(figsize =(14, 9));ax=fig.add_subplot(1,1,1)
ax.plot(times, loads, 'o', markersize=1, color="red")
ax.set_title("BC Hydro Load Data", fontsize=22)
ax.set_ylabel("Load (MW)", fontsize=20)
plt.savefig("../Plots/LoadData.png")

GHIs = power_RDD.map(lambda x: x[iGHI]).map(lambda x: x/1e6).collect()   # Convert to MW/m^2
fig = plt.figure(figsize =(14, 9));ax=fig.add_subplot(1,1,1)
ax.plot(times, GHIs, 'o', markersize=1, color="green")
ax.set_title("Total Global Horizontal Irradiance Per Unit Area\nover the 100 Selected Sites", fontsize=22)
ax.set_ylabel("GHI (MW/m$^2$)", fontsize=20)
plt.savefig("../Plots/IrrData.png")

# Plot the time variation in scaled GHI and load together
GHIs_scaled = power_RDD.map(lambda x: x[iGHI_SCALED]).collect()
fig = plt.figure(figsize =(14, 9));ax=fig.add_subplot(1,1,1)
ax.plot(times, GHIs_scaled, 'o', markersize=1, color="green", label = "GHI")
ax.plot(times, loads, 'o', markersize=1, color="red", label="Load")
ax.set_title("Comparison of BC Hydro Load and Scaled GHI", fontsize=22)
ax.set_ylabel("Power (MW)", fontsize=20)
ax.legend(prop={'size': 20}, markerscale=10)
plt.savefig("../Plots/PowerComparison.png")

# Plot the time variation in cumulative scaled GHI and load together, with the difference in a sub-panel
loads_cumsum = power_RDD.map(lambda x: 1.0*x[iLOAD_CUMSUM]/1e6).collect()
GHIs_cumsum_scaled = power_RDD.map(lambda x: x[iGHI_CUMSUM_SCALED]/1e6).collect()
diffs = power_RDD.map(lambda x: x[iDIFF]/1e6).collect()
gs=GridSpec(3,1) # 3 rows, 1 columns
fig = plt.figure(figsize =(14, 14));ax1=fig.add_subplot(gs[0:2,0])
ax1.plot(times, GHIs_cumsum_scaled, color="green", label = "GHI")
ax1.plot(times, loads_cumsum, color="red", label="Load")
ax1.set_title("Comparison of Cumulative BC Hydro Load and Scaled GHI, with GHI Offset", fontsize=22)
ax1.set_ylabel("Energy (TWh)", fontsize=20)
ax1.legend(prop={'size': 20})
ax2=fig.add_subplot(gs[2,0], sharex=ax1)
ax2.plot(times, diffs, color="blue")
ax2.set_ylabel("GHI-Load", fontsize=20)
ax2.axhline(0, linestyle="--", color="black")
plt.savefig("../Plots/EnergyComparison.png")

# Plot the time variation in cumulative scaled GHI and load together, with the difference in a sub-panel, before adding offset
loads_cumsum = power_RDD.map(lambda x: 1.0*x[iLOAD_CUMSUM]/1e6).collect()
GHIs_cumsum_scaled = power_RDD.map(lambda x: (x[iGHI_CUMSUM_SCALED]+min_diff)/1e6).collect()
diffs = power_RDD.map(lambda x: (x[iDIFF]+min_diff)/1e6).collect()
gs=GridSpec(3,1) # 3 rows, 1 columns                                                                                                                                                
fig = plt.figure(figsize =(14, 14));ax1=fig.add_subplot(gs[0:2,0])
ax1.plot(times, GHIs_cumsum_scaled, color="green", label = "GHI")
ax1.plot(times, loads_cumsum, color="red", label="Load")
ax1.set_title("Comparison of Cumulative BC Hydro Load and Scaled GHI, without GHI Offset", fontsize=22)
ax1.set_ylabel("Energy (TWh)", fontsize=20)
ax1.legend(prop={'size': 20})
ax2=fig.add_subplot(gs[2,0], sharex=ax1)
ax2.plot(times, diffs, color="blue")
ax2.set_ylabel("GHI-Load", fontsize=20)
ax2.axhline(0, linestyle="--", color="black")
plt.savefig("../Plots/EnergyComparison_BeforeShift.png")

print("Time Elapsed: %ds"%(time.time()-start_time))
