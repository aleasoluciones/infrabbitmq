# -*- coding: utf-8 -*-

import importlib
import os
import sys
import argparse
from infcommon import logger, utils
from infrabbitmq import (
    factory,
    rabbitmq,
    jsonserializer,
)


import time
import datetime


class Importer(object):

    @classmethod
    def import_module(cls, module_name):
        return importlib.import_module(module_name)

    @classmethod
    def get_symbol(cls, symbol_name):
        module_name = cls._extract_module_name(symbol_name)
        final_symbol_name = cls._extract_final_symbol_name(symbol_name)
        module = importlib.import_module(module_name)
        try:
            return getattr(module, final_symbol_name)
        except AttributeError:
            raise ImportError()

    @classmethod
    def _extract_module_name(cls, symbol_name):
        return '.'.join(symbol_name.split('.')[:-1])

    @classmethod
    def _extract_final_symbol_name(cls, symbol_name):
        return symbol_name.split('.')[-1:][0]


def event_processor_name(factory_func_name):
    return factory_func_name.split('.')[-1:]

class LogProcessor(object):
    def __init__(self, processor):
        self._processor = processor

    def process(self, event):
        logger.debug("Processor {} processing {}".format(self._processor.__class__.__name__, event))
        self._processor.process(event)

class NoopProcessor(object):
    def process(self, event):
        pass


def _queue_event_processor(queue, exchange, topics, event_processor, message_ttl, serializer):
    return factory.rabbitmq_queue_event_processor(
        queue,
        exchange,
        topics,
        event_processor,
        message_ttl,
        serializer)


def _process_body_events(queue, exchange, topics, event_processor, message_ttl, serializer):
    logger.info("Connecting")
    _queue_event_processor(queue, exchange, topics, event_processor, message_ttl, serializer).process_body()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--factory', action='store', required=False, help='')
    parser.add_argument('-e', '--exchange', action='store', required=True, help='')
    parser.add_argument('-q', '--queue', action='store', required=True, help='')
    parser.add_argument('-ttl', '--message-ttl', action='store', type=int, default=None, help='In milliseconds!')
    parser.add_argument('-t', '--topics', nargs='+', action='store', required=True, help='')
    args = parser.parse_args()

    try:
        if args.factory:
            event_processor_symbol = Importer.get_symbol(args.factory)
            event_processor = event_processor_symbol()
            processor_name = event_processor_name(args.factory)
        else:
            event_processor = NoopProcessor()
            processor_name = event_processor.__class__.__name__

        serializer = factory.json_serializer()

        logger.info("(%d) Starting event_processor %s" % (os.getpid(), processor_name))
        logger.info("%s queue %s topics %s" % (processor_name, args.queue, args.topics))
        logger.info("%s deserializer %s" % (processor_name, serializer))

        utils.do_stuff_with_exponential_backoff((rabbitmq.RabbitMQError,),
            _process_body_events,
            args.queue,
            args.exchange,
            args.topics,
            LogProcessor(event_processor),
            args.message_ttl,
            serializer)
    except Exception as exc:
        logger.error('Uncontrolled exception: {exc}'.format(exc=exc), exc_info=True)
        sys.exit(-1)


if __name__ == '__main__':
    main()
