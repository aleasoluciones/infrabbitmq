#!/bin/bash

echo
echo "Running Integration tests"
echo "----------------------------------------------------------------------"
echo
nosetests $INTEGRATION_TESTS -s --logging-clear-handlers --processes=16 --process-timeout=50
NOSE_RETCODE=$?

exit $NOSE_RETCODE
