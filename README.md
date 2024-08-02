[![DOI](https://zenodo.org/badge/175055800.svg)](https://zenodo.org/doi/10.5281/zenodo.13173331)

# SolarProject_BC

## Requirements
  * python3 with pip3
  * Hadoop 2.8.5
  * spark 2.4.0 with pyspark
  * gnumeric (for ubuntu: sudo apt-get install -y gnumeric)
   
   
## 1. Install python modules and download data
  
1. Install the required python modules as follows:
  
~~~~
cd python
sudo pip3 install -r Requirements.txt
cd ..
~~~~
    
2. Create the Tables directory and sub-directories. If there's enough free space (~30 GB) in the root disk, it can be created directly in the top-level directory of the git repo:
  
~~~~
mkdir Tables
mkdir Tables/IrradianceData
mkdir Tables/IrradianceData_isInBC
mkdir Tables/LoadData
mkdir Tables/ContourData
~~~~
  
If there isn't enough free space, it can be created on another disk and linked from the top-level directory of the git repo:
  
~~~~
# For example, the Tables directory is created here on a disk mounted in /mntc with 98 GB of free space:
mkdir /mntc/Tables
mkdir /mntc/Tables/IrradianceData
mkdir /mntc/Tables/IrradianceData_isInBC
mkdir Tables/LoadData
mkdir Tables/ContourData
ln -s /mntc/Tables Tables
~~~~
  
3. From the top-level directory of the git repo (SolarProject_BC), run the bash script MakePwdFiles.sh to make ".PWD" files in each directory and sub-directory of the repo, which will make it possible to run the code from anywhere in the repo. 

**NOTE: This is the only script that MUST be run from the top-level directory of the repo.
  
~~~~
bash/MakePwdFiles.sh
~~~~
  
4. Run DownloadIrradianceFiles.sh to copy and unzip the zipped irradiance files from ftp://ftp.nrcan.gc.ca with longitudes ranging to the furthest east extent of BC to the Tables/IrradianceData directory:
  
~~~~
bash/DownloadIrradianceFiles.sh
~~~~

This should take ~30 minutes or so, depending on the internet speed.
  
5. Download the load data from 2002 to 2008 from the BC hydro website:

~~~~
bash/DownloadLoadFiles.sh
~~~~

This should take less than a minute. 
  
6. Download and unzip the .shp file containing contour data for Canadian provincial borders:

~~~~
bash/DownloadContourData.sh
~~~~

This should also take less than a minute.


## 2. Copy irradiance files with BC coordinates to HDFS

1. Make a list of filenames for the downloaded irradiance files, from which the geographical longitude and latitude of the data can be parsed:

~~~~
bash/MakeListOfIrradianceFiles.sh
~~~~

2. Run the python script CopyIrradianceData2HDFS.py to copy the solar irradiance files with coordinates lying within BC into HDFS:

~~~~
python/CopyIrradianceData2HDFS.py
~~~~

## 3. Perform the analysis to determine the minimum required storage capacity

1. Run the python script CalculateAverageGHI.py to create and save a dataframe for all the data lying in BC containing a set of coordinates, a time stamp, and the GHI (global horizontal irradiance) on each row, and also a dataframe containing the average GHI for each set of coordinates.

~~~~
python/CalculateAverageGHI.py
~~~~

2. Run the python script SelectRandomCoords.py to sort the dataframe saved by MapIntegratedIrradiances.py containing the coordinates and average GHI according to average GHI, and randomly select 100 indices from a Gaussian distribution peaked at the maximum number of indices for the solar farm sites. 

~~~~
python/SelectRandomCoords.py
~~~~

A map of the average GHI over BC is also plotted, along with the randomly chosen solar farm sites.

3. Run CalculateCumSum.py to combine the dataframe containing coordinates time stamps, and GHIs with an equivalent dataframe containing the BC hydro load, and scale the GHI column such that the cumulative sums of the GHI and load columns match.

~~~~
python/CalculateCumSum.py
~~~~

4. Run AnalyzeResults.py to visualize the GHI and load data, as well as their cumulative sums (i.e. accumulated energy), and use the maximum difference between the cumulative GHI and load sums to determine the maximum required storage capacity.

~~~~
python/AnalyzeResults.py
~~~~
