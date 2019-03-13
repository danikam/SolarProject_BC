#!/usr/bin/env python3                                                                                                                                                               
import findspark
findspark.init()
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as f
from pyspark.sql.types import FloatType
import matplotlib.pyplot as plt
from matplotlib import cm
import time
import geopandas as gpd
import numpy as np

sc=SparkContext()
sql=SQLContext(sc)

start_time = time.time()

# Read in the dataframe of average GHI per set of coordinates
irr_avg_DF = sql.read.format('csv').load("/user/ubuntu/IrradianceMap/AverageIrradiance_df", inferSchema=True)
irr_avg_DF = irr_avg_DF.withColumnRenamed("_c0", "Lat")
irr_avg_DF = irr_avg_DF.withColumnRenamed("_c1", "Long")
irr_avg_DF = irr_avg_DF.withColumnRenamed("_c2", "GHI_Avg")
irr_avg_DF.show(n=10)

# Function to randomize the GHI for each coordinate by multiplying it by a random number between 0.9 and 1.1
def randomize_GHI(GHI):
    np.random.seed(0)
    return np.random.uniform(0.9, 1.1) * GHI
    
# Make a new column with the average GHI multiplied by a random number between 0.9 and 1.1 
randomize_GHI_udf = f.udf(randomize_GHI, FloatType())
irr_avg_DF_sample = irr_avg_DF.withColumn("GHI_Avg_Rand", randomize_GHI_udf(f.col("GHI_Avg"))).sort("GHI_Avg_Rand", ascending=False).limit(100)
#irr_avg_DF_sample.show(n=10)

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
