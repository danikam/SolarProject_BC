#!/bin/bash
# Date:     190311
# Purpose:  Make a list of the irradiance file names in Tables/IrradianceData to be used for selecting the files within BC.

cd ../Tables/IrradianceData
ls *.csv > filenames.txt
