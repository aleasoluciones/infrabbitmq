# -*- coding: utf-8 -*-

from doublex import *
from expects import *
from doublex_expects import *

import os

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

with describe('Rabbitmq object even proccesor specs'):
    with before.each:
        self.exchange = 'IRRELEVANT_EX_%s' % os.getpid()
        self.queue = 'IRRELEVANT_QUEUE_%s' % os.getpid()
        self.rabbitmq_client = factory.rabbitmq_client()
        self.event_publisher = factory.event_publisher(self.exchange)
        self.processor = Spy()

    with after.each:
        try:
            self.rabbitmq_client.queue_delete(queue=self.queue)
        except:
            pass
        try:
            self.rabbitmq_client.exchange_delete(self.exchange)
        except:
            pass

    with context('processing an event'):
        with it('calls the processor with event object data'):
            event = FakeEvent(TOPIC, NETWORK, DATA)
            queue_event_processor = factory.rabbitmq_queue_event_processor(self.queue, self.exchange, '#', self.processor, event_builder=factory.raw_event_builder)

            self.event_publisher.publish_event_object(event)
            self.event_publisher.publish_event_object(event)

            queue_event_processor.process_body(max_iterations=1)

            expect(self.processor.process).to(have_been_called_with(have_key('_data', equal(DATA))).once)
