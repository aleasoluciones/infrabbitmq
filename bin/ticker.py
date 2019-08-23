# -*- coding: utf-8 -*-

import time
import puka
import argparse


import logging
from infcommon import utils
from infrabbitmq import factory as infrabbitmq_factory
from infrabbitmq.rabbitmq import RabbitMQError
from infrabbitmq.events_names import (
    TICK_1_SECOND,
    TICK_1_MINUTE,
    TICK_2_MINUTES,
    TICK_5_MINUTES,
    TICK_60_MINUTES,
)


def publish_event(publisher, event, network, secs, mins):
    logging.info("publish event {} {}".format(event, secs))
    publisher.publish(event, network, data={'tick': secs, 'mins': mins})


def main(network):
    publisher = infrabbitmq_factory.event_publisher_json_serializer()
    secs = 0
    mins = 0

    rabbitmq_exceptions = (RabbitMQError, puka.AMQPError, KeyError,)
    while True:
        time.sleep(1)
        secs += 1

        utils.do_stuff_with_exponential_backoff(rabbitmq_exceptions,
            publish_event,
            publisher, TICK_1_SECOND, network, secs, mins)

        if secs % 60 == 0:
            mins += 1
            secs = 0

            utils.do_stuff_with_exponential_backoff(rabbitmq_exceptions,
                    publish_event,
                    publisher, TICK_1_MINUTE, network, secs, mins)

            if mins % 2 == 0:
                utils.do_stuff_with_exponential_backoff(rabbitmq_exceptions,
                    publish_event,
                    publisher, TICK_2_MINUTES, network, secs, mins)

            if mins % 5 == 0:
                utils.do_stuff_with_exponential_backoff(rabbitmq_exceptions,
                    publish_event,
                    publisher, TICK_5_MINUTES, network, secs, mins)

            if mins % 60 == 0:
                utils.do_stuff_with_exponential_backoff(rabbitmq_exceptions,
                    publish_event,
                    publisher, TICK_60_MINUTES, network, secs, mins)


if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('-n', '--network', action='store', required=True, help='Network name (ilo, c2k, ...)')
        args = parser.parse_args()
        network = args.network.split('-')[0]

        main(network)
    except Exception as exc:
        logging.critical("Ticker Fails: {}".format(exc))
