# -*- coding: utf-8 -*-

from __future__ import absolute_import

import os
import infcommon
from infcommon import clock, logger
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


def rabbitmq_queue_event_processor(queue_name, exchange, topics, event_processor, message_ttl, serializer=None):
    return rabbitmq.RabbitMQQueueEventProcessor(queue_name,
                                                event_processor,
                                                rabbitmq_client(serializer=serializer),
                                                exception_publisher=logger,
                                                exchange=exchange,
                                                topics=topics,
                                                message_ttl=message_ttl)

def rabbitmq_client(broker_uri=None, serializer=None):
    broker_uri = broker_uri or os.environ['BROKER_URI']
    serializer = json_serializer()
    return rabbitmq.RabbitMQClient(broker_uri, serializer)

