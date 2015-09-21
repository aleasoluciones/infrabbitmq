# -*- coding: utf-8 -*-

import logging
import argparse
from infrabbitmq import factory
import os

parser = argparse.ArgumentParser()
parser.add_argument("queue_name")
args = parser.parse_args()

rabbitmq_client = factory.rabbitmq_client()
logging.info("Deleteting {queue_name}".format(queue_name=args.queue_name))
rabbitmq_client.queue_delete(args.queue_name)
logging.info("Deleted {queue_name}".format(queue_name=args.queue_name))
