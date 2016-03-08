#!/bin/bash

find . -name *.pyc -delete
source "dev/env_develop"
echo
echo "Running tests"
echo "----------------------------------------------------------------------"
echo
nosetests `find . -maxdepth 2 -type d -name "tests" | grep -v systems` --logging-clear-handlers -s "$@"
NOSE_RETCODE=$?

exit $NOSE_RETCODE