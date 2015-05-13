# -*- coding: utf-8 -*-

import unittest
import os
from time import sleep
from doublex import *
from hamcrest import *
from infrabbitmq import (
    factory,
    events,
    rabbitmq
)

IRRELEVANT_NETWORK = 'irrelevant_network'

class RabbitMQQueueEventProcessorTest(unittest.TestCase):

    def setUp(self):
        self.exchange = 'IRRELEVANT_EX_%s' % os.getpid()
        self.queue = 'IRRELEVANT_QUEUE_%s' % os.getpid()
        self.rabbitmq_client = factory.rabbitmq_client()
        self.event_publisher = factory.event_publisher(self.exchange)
        self.processor = Spy()

    def test_process_body_event(self):
        queue_event_processor = self._queue_event_processor_with_topics('#')

        self.event_publisher.publish('kern.critical', IRRELEVANT_NETWORK, data={})
        self.event_publisher.publish('kern.critical.info', IRRELEVANT_NETWORK, data={})

        queue_event_processor.process_body(max_iterations=2)

        assert_that(self.processor.process, called().with_args(instance_of(events.Event)).times(2))

    def test_process_body_events_from_multiple_topics(self):
        queue_event_processor = self._queue_event_processor_with_topics('*.kern.critical', '*.hdd.info')

        self.event_publisher.publish('kern.critical', IRRELEVANT_NETWORK, data={})
        self.event_publisher.publish('hdd.info', IRRELEVANT_NETWORK, data={})

        queue_event_processor.process_body(max_iterations=2)

        assert_that(self.processor.process, called().with_args(instance_of(events.Event)).times(2))

    def test_process_body_events_per_queue_message_ttl(self):
        queue_event_processor = self._queue_event_processor_with_topics('#', message_ttl=1000)

        self.event_publisher.publish('kern.critical', IRRELEVANT_NETWORK, data={})

        sleep(2)
        queue_event_processor.process_body(max_iterations=1)

        assert_that(self.processor.process, never(called()))

    def _queue_event_processor_with_topics(self, *topics, **kwargs):
        return factory.rabbitmq_queue_event_processor(self.queue, self.exchange, topics, self.processor,
            kwargs.get('message_ttl'))

    def tearDown(self):
        self._delete_resources()

    def _delete_resources(self):
        try:
            self.rabbitmq_client.queue_delete(queue=self.queue)
        except:
            pass
        try:
            self.rabbitmq_client.exchange_delete(self.exchange)
        except:
            pass
