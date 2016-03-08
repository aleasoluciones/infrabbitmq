# -*- coding: utf-8 -*-

from __future__ import absolute_import

import logging
import os
import infcommon
from infcommon import clock
from infrabbitmq import (
    events,
    jsonserializer,
    rabbitmq,
)


def json_serializer():
    return infcommon.Factory.instance('json_serializer', lambda: jsonserializer.JsonSerializer())

def event_publisher(exchange='events', broker_uri=None):
    return infcommon.Factory.instance('event_publisher_%s_%s' % (exchange, broker_uri),
        lambda: rabbitmq.EventPublisher(
            rabbitmq_client(broker_uri=broker_uri),
            clock.Clock(),
            exchange=exchange)
    )


def rabbitmq_queue_event_processor(queue_name, exchange, topics, processor, serializer=None, queue_options=None, exchange_options=None, event_builder=None):
    if event_builder is None:
        event_builder = felix_event_builder

    return rabbitmq.RabbitMQQueueEventProcessor(queue_name=queue_name,
                                                processor=processor,
                                                rabbitmq_client=rabbitmq_client(serializer=serializer),
                                                exchange=exchange,
                                                topics=topics,
                                                exchange_options=exchange_options or {},
                                                queue_options=queue_options or {},
                                                event_builder=event_builder)



def felix_event_builder(raw_event):
    return events.Event(**raw_event)


def raw_event_builder(raw_event):
    return raw_event


def rabbitmq_client(broker_uri=None, serializer=None):
    broker_uri = broker_uri or os.environ['BROKER_URI']
    serializer = json_serializer()
    return rabbitmq.RabbitMQClient(broker_uri, serializer)

