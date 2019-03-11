#!/bin/bash
# Date:     190311
# Purpose:  Make a list of the irradiance file names in Tables/IrradianceData to be used for selecting the files within BC.

# Get the path to the top-level directory of the repo
repo_dir=`cat .PWD`

cd ${repo_dir}/Tables/IrradianceData
ls *.csv > filenames.txt
