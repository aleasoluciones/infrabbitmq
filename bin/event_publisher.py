# -*- coding: utf-8 -*-

import argparse
from infrabbitmq import factory
import os
import ast


parser = argparse.ArgumentParser()
parser.add_argument('-d', '--destination_exchange', action='store', required=True, help='')
parser.add_argument('-e', '--event_name', action='store', default='events', help='')
parser.add_argument('-n', '--network', action='store', required=True, help='Network name (ilo, c2k, ...)')
parser.add_argument('-o', '--operations', action='store_true', default=False, help='Publish to operations broker')
parser.add_argument("data")
args = parser.parse_args()


if args.operations:
    broker_uri = os.environ['OPERATIONS_BROKER_URI']
else:
    broker_uri = os.environ['BROKER_URI']

rabbitmq_client = factory.event_publisher_json_serializer(args.destination_exchange, broker_uri=broker_uri)
data_in_dictionary_format = ast.literal_eval(args.data)
rabbitmq_client.publish(args.event_name, args.network, data_in_dictionary_format)
