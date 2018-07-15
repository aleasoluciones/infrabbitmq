#!/bin/bash

find . -name *pyc* -delete
source "dev/env_develop"
INTEGRATION_TESTS=`find . -maxdepth 2 -type d -name "integration_tests"`

echo "Starting rabbitmq container..."
docker run -d --hostname infrabbit --name infrabbit -e RABBITMQ_DEFAULT_USER=infrabbit -e RABBITMQ_DEFAULT_PASS=infrabbit -p 15672:15672 -p 5672:5672 aleasoluciones/rabbitmq-delayed-message:0.2
sleep 5
echo -n "."
sleep 5
echo -n ".."
sleep 5
echo -n "..."
sleep 5
echo -n "...."
echo "Ready!"
echo
echo "Running Integration tests"
echo "----------------------------------------------------------------------"
echo
mamba -f documentation $INTEGRATION_TESTS

RETCODE=$NOSE_RETCODE
sleep 1
IMAGE_ID=$(docker stop $(docker ps | grep infrabbit | awk '{print $1}'))
docker rm $IMAGE_ID
exit $RETCODE
