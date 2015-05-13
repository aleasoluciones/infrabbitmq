# -*- coding: utf-8 -*-

import argparse
from infrastructure import factory
from infrastructure import logger

parser = argparse.ArgumentParser()
parser.add_argument("queue_name")
parser.add_argument("exchange")
parser.add_argument("routing_key")
args = parser.parse_args()

rabbitmq_client = factory.rabbitmq_client()
logger.info("Unbind queue: {} ex: {} routing: {}".format(args.queue_name, args.exchange, args.routing_key)
rabbitmq_client.queue_unbind(args.queue_name, args.exchange, args.routing_key)
logger.info("Unbinding complete")

