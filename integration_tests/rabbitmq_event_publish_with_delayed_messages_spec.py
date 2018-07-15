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

    with context('processing a delayed event'):
        with it('calls the processor with event object data after delayed time'):
            delay = 5000
            queue_event_processor = factory.rabbitmq_queue_event_processor(self.queue, self.exchange, '#', self.processor, exchange_type=rabbitmq.X_DELAYED)

            self.event_publisher.publish_with_delay('kern.critical', IRRELEVANT_NETWORK, delay, data={})

            queue_event_processor.process_body(max_iterations=1)

            expect(self.processor.process).not_to(have_been_called)

            sleep(5)

            queue_event_processor.process_body(max_iterations=1)

            expect(self.processor.process).to(have_been_called_with(be_a(events.Event)).once)

    with context('processing a delayed event with value 0'):
        with it('calls the processor with event object immediately'):
            delay = 0
            queue_event_processor = factory.rabbitmq_queue_event_processor(self.queue, self.exchange, '#', self.processor, exchange_type=rabbitmq.X_DELAYED)

            self.event_publisher.publish_with_delay('kern.critical', IRRELEVANT_NETWORK, delay, data={})

            queue_event_processor.process_body(max_iterations=1)

            expect(self.processor.process).to(have_been_called_with(be_a(events.Event)).once)
