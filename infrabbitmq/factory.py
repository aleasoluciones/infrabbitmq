# -*- coding: utf-8 -*-

from __future__ import absolute_import

import os
from infcommon.factory import Factory
from infcommon.serializer import factory as serializer_factory
from infcommon import clock
from infrabbitmq import (
    events,
    rabbitmq,
)


def event_publisher(exchange='events', broker_uri=None):
    return Factory.instance('event_publisher_{}_{}'.format(exchange, broker_uri),
                            lambda: rabbitmq.EventPublisher(rabbitmq_client(broker_uri=broker_uri),
                                                            clock.Clock(),
                                                            exchange=exchange)
                            )


def event_publisher_pickle_serializer(exchange='events', broker_uri=None):
    return Factory.instance('event_publisher_pickle_serializer_{}_{}'.format(exchange, broker_uri),
                            lambda: rabbitmq.EventPublisher(rabbitmq_client(broker_uri=broker_uri,
                                                                            serializer=serializer_factory.pickle_serializer()),
                                                            clock.Clock(),
                                                            exchange=exchange)
                            )


def event_publisher_json_serializer(exchange='events', broker_uri=None):
    return Factory.instance('event_publisher_json_serializer{}_{}'.format(exchange, broker_uri),
                            lambda: rabbitmq.EventPublisher(rabbitmq_client(broker_uri=broker_uri,
                                                                            serializer=serializer_factory.json_serializer()),
                                                            clock.Clock(),
                                                            exchange=exchange)
                            )


def rabbitmq_queue_event_processor(queue_name, exchange, topics, processor, serializer=None, queue_options=None, exchange_options=None, event_builder=None, exchange_type=rabbitmq.TOPIC):
    if event_builder is None:
        event_builder = felix_event_builder

    if serializer is None:
        serializer = serializer_factory.json_or_pickle_serializer()

    return rabbitmq.RabbitMQQueueEventProcessor(queue_name=queue_name,
                                                processor=processor,
                                                rabbitmq_client=rabbitmq_client(serializer=serializer),
                                                exchange=exchange,
                                                topics=topics,
                                                exchange_options=exchange_options or {},
                                                queue_options=queue_options or {},
                                                event_builder=event_builder,
                                                exchange_type=exchange_type)


def felix_event_builder(raw_event):
    return events.Event(**raw_event)


def raw_event_builder(raw_event):
    return raw_event


def rabbitmq_client(broker_uri=None, serializer=None):
    broker_uri = broker_uri or os.environ['BROKER_URI']
    serializer = serializer or serializer_factory.json_serializer()
    return rabbitmq.RabbitMQClient(broker_uri, serializer)


def console_log_events_processor(*args):
    return events.ConsoleLogEventsProcessor()
