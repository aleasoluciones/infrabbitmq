# -*- coding: utf-8 -*-

import argparse
from infrabbitmq import factory
from infcommon import logger
import os

parser = argparse.ArgumentParser()
parser.add_argument("queue_name")
args = parser.parse_args()

rabbitmq_client = factory.rabbitmq_client()
logger.info("Deleteting {queue_name}".format(queue_name=args.queue_name))
rabbitmq_client.queue_delete(args.queue_name)
logger.info("Deleted {queue_name}".format(queue_name=args.queue_name))

