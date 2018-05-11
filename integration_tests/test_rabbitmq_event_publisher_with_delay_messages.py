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

class RabbitMQEventPublisherWithDelayMessagesTest(unittest.TestCase):

    def setUp(self):
        self.exchange = 'IRRELEVANT_EX_%s' % os.getpid()
        self.queue = 'IRRELEVANT_QUEUE_%s' % os.getpid()
        self.rabbitmq_client = factory.rabbitmq_client()
        self.event_publisher = factory.event_publisher(self.exchange)
        self.processor = Spy()

    def test_process_delayed_event(self):
        delay = 5000
        queue_event_processor = factory.rabbitmq_queue_event_processor(self.queue, self.exchange, '#', self.processor, exchange_type=rabbitmq.X_DELAYED)

        self.event_publisher.publish_with_delay('kern.critical', IRRELEVANT_NETWORK, delay, data={})

        queue_event_processor.process_body(max_iterations=1)

        assert_that(self.processor.process, never(called()))

        sleep(5)

        queue_event_processor.process_body(max_iterations=1)

        assert_that(self.processor.process, called().with_args(instance_of(events.Event)).times(1))

    def test_process_zero_milliseconds_delayed_event(self):
        delay = 0
        queue_event_processor = factory.rabbitmq_queue_event_processor(self.queue, self.exchange, '#', self.processor, exchange_type=rabbitmq.X_DELAYED)

        self.event_publisher.publish_with_delay('kern.critical', IRRELEVANT_NETWORK, delay, data={})

        queue_event_processor.process_body(max_iterations=1)

        assert_that(self.processor.process, called().with_args(instance_of(events.Event)).times(1))

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
