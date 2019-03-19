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
import pandas as pd
import geopandas as gpd
import numpy as np
from pyspark.sql.functions import lit

sc=SparkContext()
sql=SQLContext(sc)

start_time = time.time()

# Function to randomly select 100 indices from a Gaussian distribution of indices peaked at the maximum index (N_coords-1)
def sample_gaussian(nSamples, N_coords):
    samples = np.random.normal(1,0.1, N_coords*3)
    indices_selected = np.unique(np.floor(samples[(samples<1)&(samples>0)]*N_coords))
    indices = np.random.choice(indices_selected, nSamples, replace=False)
    return indices.astype(int).tolist()

# Read in the dataframe of average GHI per set of coordinates 
irr_avg_DF = sql.read.format('csv').load("/user/ubuntu/IrradianceMap/AverageIrradiance_df", inferSchema=True)

# Rename to columns and sort in order of increasing average GHI
irr_avg_DF = irr_avg_DF.withColumnRenamed("_c0", "Lat")
irr_avg_DF = irr_avg_DF.withColumnRenamed("_c1", "Long")
irr_avg_DF = irr_avg_DF.withColumnRenamed("_c2", "GHI_Avg")
irr_avg_DF = irr_avg_DF.sort("GHI_Avg")
#irr_avg_DF.show(n=10)

# Randomly select the indices for the solar site locations, with a preference for locations with higher GHI
N_coords = irr_avg_DF.count()
selection = sample_gaussian(100, N_coords)

# Convert the dataframe to an RDD, add the index to each ntuple, and convert back to a dataframe
# so that each row of the dataframe now has a row index
irr_avg_RDD = irr_avg_DF.rdd.map(tuple)
irr_avg_RDD = irr_avg_RDD.zipWithIndex().map(lambda x: (x[0][0], x[0][1], x[0][2], x[1]))
irr_avg_DF = irr_avg_RDD.toDF(["Lat", "Long", "GHI_Avg", "Index"])

# Filter for the selected indices
irr_avg_DF_sample = irr_avg_DF.filter(irr_avg_DF.Index.isin(selection))
#irr_avg_DF_sample.show(n=5)

# Select the latitude and longitude columns to save
irr_avg_DF_sample_coords = irr_avg_DF_sample.select("Lat", "Long")

# Save the samples to csv
irr_avg_DF_sample_coords.write.save('/user/ubuntu/IrradianceMap/SampledCoords_df', format='csv', mode='overwrite')

######## Plot the amplitudes overlaid on the BC border #########

# Get the path to the top level of the
with open(".PWD") as f:
  repo_path = f.readline().strip()

# Open the .shp file and select the BC data
fp = "%s/Tables/ContourData/gadm36_CAN_1.shp"%repo_path
data_can = gpd.read_file(fp)
data_bc = data_can[data_can.NAME_1=="British Columbia"]

# Plot the BC contour
data_bc.plot(figsize=(6,3.2))
plt.xlabel("Longitude (degrees)")
plt.ylabel("Latitude (degrees)")

# Collect the latitude, longitude, and GHI columns of the average GHI dataframe as lists
Lats = irr_avg_DF.select("Lat").collect()
Longs = irr_avg_DF.select("Long").collect()
GHIs = irr_avg_DF.select("GHI_Avg").collect()

# Overlay the BC border with a color plot of the average GHI
plt.scatter(Longs, Lats, c=GHIs, s=0.2, cmap=cm.jet)
cbar = plt.colorbar(pad=0.02, label="Average GHI (Wh/m$^2$)")

# Plot the selected coordinates
Lats_sel = irr_avg_DF_sample_coords.select("Lat").collect()
Longs_sel = irr_avg_DF_sample_coords.select("Long").collect()
plt.scatter(Longs_sel, Lats_sel, c="black", s=1, label="Selected Sites")
plt.legend()
plt.savefig("%s/Plots/IrradianceMap.png"%repo_path)

################################################################

print("Time Elapsed: %ds"%(time.time()-start_time))
