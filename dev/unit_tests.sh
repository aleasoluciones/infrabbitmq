#!/bin/bash

find . -name *.pyc -delete
source "dev/env_develop"

echo
echo "----------------------------------------------------------------------"
echo "Running Specs"
echo "----------------------------------------------------------------------"
echo
mamba -f progress `find . -maxdepth 2 -type d -name "specs" | grep -v systems`
MAMBA_RETCODE=$?


exit $(($MAMBA_RETCODE))