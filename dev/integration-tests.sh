#!/bin/bash

find . -name *pyc* -delete
source "dev/env_develop"
INTEGRATION_TESTS=`find . -maxdepth 2 -type d -name "integration_tests"`

echo
echo "Running Integration tests"
echo "----------------------------------------------------------------------"
echo
nosetests $INTEGRATION_TESTS -s --logging-clear-handlers --processes=16 --process-timeout=50
NOSE_RETCODE=$?

RETCODE=$NOSE_RETCODE
sleep 1
exit $RETCODE
