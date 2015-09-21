# -*- coding: utf-8 -*-

import logging
import argparse
from infrabbitmq import factory

parser = argparse.ArgumentParser()
parser.add_argument("queue_name")
parser.add_argument("exchange")
parser.add_argument("routing_key")
args = parser.parse_args()

rabbitmq_client = factory.rabbitmq_client()
logging.info("Unbind queue: {} ex: {} routing: {}".format(args.queue_name, args.exchange, args.routing_key))
rabbitmq_client.queue_unbind(args.queue_name, args.exchange, args.routing_key)
logging.info("Unbinding complete")

