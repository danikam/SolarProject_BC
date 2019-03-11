#!/bin/bash
# Date:     190311
# Purpose:  Create files called .pwd in each directory so the code can be run from anywhere. This script MUST be run from the top directory of the git repo.

echo $PWD > .PWD
find . -type d -exec cp .PWD {} \;
