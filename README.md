# SolarProject_BC

## Requirements
  * python3 with pip3
  * pyspark
  * gnumeric (for ubuntu: sudo apt-get install -y gnumeric)
   
   
## Install python modules and download data
  
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
ln -s /mntc/Tables Tables
~~~~
  
3. From the top-level directory of the git repo (SolarProject_BC), run the bash script MakePwdFiles.sh to make ".PWD" files in each directory and sub-directory of the repo, which will make it possible to run the code from anywhere in the repo.
  
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
  
6. Download and unzip the .shp file containing contour data for Canadian provincial borders:

~~~~
bash/DownloadContourData.sh
~~~~


## Copy irradiance files with BC coordinates to HDFS

1. Make a list of filenames for the downloaded irradiance files, from which the geographical longitude and latitude of the data can be parsed:

~~~~
bash/MakeListOfIrradianceFiles.sh
~~~~

2. 
