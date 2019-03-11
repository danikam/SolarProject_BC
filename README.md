# SolarProject_BC

## Requirements
  * python3 with pip
  * pyspark
  * gnumeric (for ubuntu: sudo apt-get install -y gnumeric)
   
   
## Install Python Modules and Download Data
  
  1. Install the required python modules as follows:
  
  ~~~~
  cd python
  pip install -r Requirements.txt
  cd ..
  ~~~~
  
  (may need to use pip3 if pip isn't connected to your python3 installation).
  
  2. From the top-level directory of the git repo (SolarProject_BC), run the bash script MakePwdFiles.sh to make ".PWD" files in each directory and sub-directory of the repo, which will make it possible to run the code from anywhere in the repo.
  
  ~~~~
  bash/MakePwdFiles.sh
  ~~~~
