# -*- coding: utf-8 -*-

import importlib
import os
import sys
import argparse
import logging

from infcommon import utils, logging_utils

from infrabbitmq import factory, rabbitmq


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
        logging.debug("Processor {} processing {}".format(self._processor.__class__.__name__, event))
        self._processor.process(event)


class NoopProcessor(object):
    def process(self, event):
        pass


def _queue_event_processor(queue, exchange, topics, event_processor, message_ttl, serializer, event_builder, exchange_type):
    return factory.rabbitmq_queue_event_processor(
        queue,
        exchange,
        topics,
        event_processor,
        queue_options={'message_ttl': message_ttl},
        serializer=serializer,
        event_builder=event_builder,
        exchange_type=exchange_type)


def _process_body_events(queue, exchange, topics, event_processor, message_ttl, serializer, event_builder, exchange_type):
    logging.info("Connecting")
    _queue_event_processor(queue, exchange, topics, event_processor, message_ttl, serializer, event_builder, exchange_type).process_body()


def _configure_sentry():
    sentry_conf = {
                'level': 'CRITICAL',
                'class': 'raven.handlers.logging.SentryHandler',
                'dsn': os.getenv('SENTRY_DSN')
    }
    logging_utils.add_handler('sentry', sentry_conf)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--factory', action='store', required=False, help='')
    parser.add_argument('-b', '--event-builder', action='store', required=False, default='infrabbitmq.factory.felix_event_builder', help='Python function to build an event object')
    parser.add_argument('-e', '--exchange', action='store', required=True, help='')
    parser.add_argument('-et', '--exchange-type', action='store', required=False, help='Select exchange type: topic, direct, x-delayed-message')
    parser.add_argument('-q', '--queue', action='store', required=True, help='')
    parser.add_argument('-ttl', '--message-ttl', action='store', type=int, default=None, help='In milliseconds!')
    parser.add_argument('-t', '--topics', nargs='+', action='store', required=True, help='')
    parser.add_argument('-n', '--network', action='store', required=False, help='')
    parser.add_argument('-s', '--serialization', action="store", required=False, help="Select serialization Json, Pickle")
    args = parser.parse_args()

    _configure_sentry()

    try:
        if args.factory:
            event_processor_class = Importer.get_symbol(args.factory)
            event_processor = event_processor_class(args.network) if args.network else event_processor_class()
            processor_name = event_processor_name(args.factory)
        else:
            event_processor = NoopProcessor()
            processor_name = event_processor.__class__.__name__

        serializer = None
        if args.serialization == 'json':
            serializer = factory.json_serializer()
        if args.serialization == 'pikle':
            serializer = factory.pickle_serializer()

        exchange_type = rabbitmq.TOPIC
        if args.exchange_type:
            exchange_type = args.exchange_type

        event_builder = Importer.get_symbol(args.event_builder)

        logging.info("(%d) Starting event_processor %s" % (os.getpid(), processor_name))
        logging.info("%s queue %s topics %s" % (processor_name, args.queue, args.topics))
        logging.info("%s deserializer %s" % (processor_name, serializer))

        utils.do_stuff_with_exponential_backoff((rabbitmq.RabbitMQError,),
            _process_body_events,
            args.queue,
            args.exchange,
            args.topics,
            LogProcessor(event_processor),
            args.message_ttl,
            serializer,
            event_builder,
            exchange_type)
    except Exception as exc:
        logging.critical('Uncontrolled exception: {exc}'.format(exc=exc), exc_info=True)
        sys.exit(-1)


if __name__ == '__main__':
    main()
