#!/bin/bash

find . -name *.pyc -delete
source "dev/env_develop"
echo
echo "Running unit tests"
echo "----------------------------------------------------------------------"
echo
dev/unit_tests.sh
UNIT_TESTS_RETCODE=$?
echo "Running integration tests"
echo "----------------------------------------------------------------------"
echo
dev/integration_tests.sh
INTEGRATION_TESTS_RETCODE=$?
exit $($UNIT_TESTS_RETCODE || $INTEGRATION_TEST_RETCODE)
