#!/bin/bash

find . -name *.pyc -delete
source "dev/env_develop"
echo
echo "Running unit tests"
echo "----------------------------------------------------------------------"
echo
mamba -f documentation specs/*
UNIT_TEST_RETCODE=$?
if [ $UNIT_TEST_RETCODE != 0 ]
then
    exit $UNIT_TEST_RETCODE
fi
echo "Running integration tests"
echo "----------------------------------------------------------------------"
echo
$(PWD)/dev/integration-tests.sh
INTEGRATION_TEST_RETCODE=$?

exit $INTEGRATION_TEST_RETCODE
