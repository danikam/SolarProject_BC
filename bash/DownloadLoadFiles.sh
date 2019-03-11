#!/bin/bash

# Date:     190311
# Purpose:  Download the load data from BC hydro for 2002-2008, convert the format to csv, and update the header

# Get the path to the top-level directory of the repo
repo_dir=`cat .PWD`

# cd into the folder that the data will be downloaded to
cd ${repo_dir}/Tables/LoadData


# Loop through each year, downloading the load data, converting it to csv format, and updating the header
for year in {2002..2008}
do
  # Remove the xls and csv files if they already exist
  rm BalancingAuthorityLoad${year}.xls BalancingAuthorityLoad${year}.csv

  # Download the file
  if [ $year -lt 2007 ]; then
    wget https://www.bchydro.com/content/dam/BCHydro/customer-portal/documents/corporate/suppliers/transmission-system/balancing_authority_load_data/Historical%20Transmission%20Data/BalancingAuthorityLoad${year}.xls
  else
    wget https://www.bchydro.com/content/dam/BCHydro/customer-portal/documents/corporate/suppliers/transmission-system/balancing_authority_load_data/Historical%20Transmission%20Data/${year}controlareaload.xls
    mv ${year}controlareaload.xls BalancingAuthorityLoad${year}.xls
  fi

  # Convert it to csv format
  ssconvert BalancingAuthorityLoad${year}.xls BalancingAuthorityLoad${year}.csv

  # Copy all but the first two lines to a new text file
  if [ $year -lt 2007 ]; then
    tail -n+3 BalancingAuthorityLoad${year}.csv > temp && mv temp BalancingAuthorityLoad${year}.csv
  else
    tail -n+4 BalancingAuthorityLoad${year}.csv > temp && mv temp BalancingAuthorityLoad${year}.csv
  fi

  # Append a new header that will be readable by pyspark
  echo '# Date, Hour, Load (MWh)' | cat - BalancingAuthorityLoad${year}.csv > temp && mv temp BalancingAuthorityLoad${year}.csv
done
