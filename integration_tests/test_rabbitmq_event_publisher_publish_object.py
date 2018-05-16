# -*- coding: utf-8 -*-

import unittest
import os
from time import sleep
from doublex import *
from hamcrest import *
from infrabbitmq import factory

NETWORK='a_network'
TOPIC='a_topic'
DATA = {'foo': 'foo'}


class FakeEvent(object):
    def __init__(self, topic, network, data):
        self._data = data
        self._network = network
        self._topic = topic

    @property
    def data(self):
        return self._data

    @property
    def network(self):
        return self._network

    @property
    def topic(self):
        return self._topic

class RabbitMQEventPublisherWithDelayMessagesTest(unittest.TestCase):

    def setUp(self):
        self.exchange = 'IRRELEVANT_EX_%s' % os.getpid()
        self.queue = 'IRRELEVANT_QUEUE_%s' % os.getpid()
        self.rabbitmq_client = factory.rabbitmq_client()
        self.event_publisher = factory.event_publisher(self.exchange)
        self.processor = Spy()

    def test_process_delayed_event(self):
        event = FakeEvent(TOPIC, NETWORK, DATA)
        queue_event_processor = factory.rabbitmq_queue_event_processor(self.queue, self.exchange, '#', self.processor, event_builder=factory.raw_event_builder)

        self.event_publisher.publish_event_object(event)

        queue_event_processor.process_body(max_iterations=1)

        assert_that(self.processor.process, called().times(1))

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
