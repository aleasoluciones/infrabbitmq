# -*- coding: utf-8 -*-

from doublex import *
from expects import *
from doublex_expects import *

import os
from time import sleep

from infrabbitmq import (
    factory,
    events,
    rabbitmq
)

IRRELEVANT_NETWORK = 'irrelevant_network'


with describe('Rabbitmq queue event proccesor specs'):
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

    with context('processing  events'):
        with context('when wildcard topic and publish two events'):
            with it('calls the processor twice with event object data'):
                queue_event_processor = factory.rabbitmq_queue_event_processor(self.queue, self.exchange, ['#'], self.processor, queue_options={'message_ttl': None})

                self.event_publisher.publish('kern.critical', IRRELEVANT_NETWORK, data={})
                self.event_publisher.publish('kern.critical.info', IRRELEVANT_NETWORK, data={})

                queue_event_processor.process_body(max_iterations=2)

                expect(self.processor.process).to(have_been_called_with(be_a(events.Event)).twice)

        with context('when processing from multiple topics and publish two events'):
            with it('calls the processor twice with event object data'):
                queue_event_processor = factory.rabbitmq_queue_event_processor(self.queue, self.exchange, ['*.kern.critical', '*.hdd.info'], self.processor, queue_options={'message_ttl': None})

                self.event_publisher.publish('kern.critical', IRRELEVANT_NETWORK, data={})
                self.event_publisher.publish('hdd.info', IRRELEVANT_NETWORK, data={})

                queue_event_processor.process_body(max_iterations=2)

                expect(self.processor.process).to(have_been_called_with(be_a(events.Event)).twice)

        with context('when processing events with ttl and time expired'):
            with it('does not process any event'):
                queue_event_processor = factory.rabbitmq_queue_event_processor(self.queue, self.exchange, ['#'], self.processor, queue_options={'message_ttl': 1000})

                self.event_publisher.publish('kern.critical', IRRELEVANT_NETWORK, data={})

                sleep(2)
                queue_event_processor.process_body(max_iterations=1)

                expect(self.processor.process).not_to(have_been_called)

        with context('when raw event builder is using as event builder'):
            with it('calls the process with instance of dict'):
                queue_event_processor = factory.rabbitmq_queue_event_processor(self.queue, self.exchange, ['#'], self.processor, queue_options=None, event_builder=factory.raw_event_builder)

                self.event_publisher.publish('kern.critical', IRRELEVANT_NETWORK, data={})

                queue_event_processor.process_body(max_iterations=1)

                expect(self.processor.process).to(have_been_called_with(be_a(dict)).once)


