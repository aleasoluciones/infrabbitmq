# -*- coding: utf-8 -*-

from doublex import *
from expects import *
from doublex_expects import *

import os

from infrabbitmq import rabbitmq, serializers

IRRELEVANT_QUEUE1 = 'irrelevant_queue1'
IRRELEVANT_QUEUE2 = 'irrelevant_queue2'
IRRELEVANT_QUEUE3 = 'irrelevant_queue3'

IRRELEVANT_EXCHANGE1 = 'irrelevant_exchange11'
IRRELEVANT_EXCHANGE2  = 'irrelevant_exchange2'
IRRELEVANT_EXCHANGE3 = 'irrelevant_exchange3'


IRRELEVANT_ROUTING_KEY = 'irrelevant_routing_key'
IRRELEVANT_ROUTING_KEY1 = 'irrelevant_routing_key1'
IRRELEVANT_ROUTING_KEY2 = 'irrelevant_routing_key2'
IRRELEVANT_MESSAGE = 'irrelevant_message'
IRRELEVANT_MESSAGE1 = 'irrelevant_message1'
IRRELEVANT_MESSAGE2 = 'irrelevant_message2'


with describe('Rabbitmq client specs'):
    with before.each:
        self.broker_uri = os.environ['BROKER_URI']
        self.rabbitmq_client = rabbitmq.RabbitMQClient(self.broker_uri, serializer=serializers.JsonSerializer())
        self.rabbitmq_client.exchange_declare(exchange=IRRELEVANT_EXCHANGE1, type=rabbitmq.DIRECT)
        self.rabbitmq_client.queue_declare(IRRELEVANT_QUEUE1, auto_delete=False)
        self.rabbitmq_client.queue_bind(IRRELEVANT_QUEUE1, IRRELEVANT_EXCHANGE1, routing_key=IRRELEVANT_ROUTING_KEY)

    with after.each:
        self.rabbitmq_client.queue_delete(queue=IRRELEVANT_QUEUE1)
        self.rabbitmq_client.exchange_delete(exchange=IRRELEVANT_EXCHANGE1)

    with context('when publishing a direct message'):
        with it('consumes just one time'):
            self.rabbitmq_client.publish(IRRELEVANT_EXCHANGE1, IRRELEVANT_ROUTING_KEY, IRRELEVANT_MESSAGE)

            msg = self.rabbitmq_client.consume(queue=IRRELEVANT_QUEUE1)
            expect(msg.body).to(equal(IRRELEVANT_MESSAGE))
            expect(self.rabbitmq_client.consume(queue=IRRELEVANT_QUEUE1)).to(be_none)

    with _context('when publishing more than one direct messages'):
        with it('consumes all pending messages iterating over them'):
            self.rabbitmq_client.publish(IRRELEVANT_EXCHANGE1, IRRELEVANT_ROUTING_KEY, IRRELEVANT_MESSAGE1)
            self.rabbitmq_client.publish(IRRELEVANT_EXCHANGE1, IRRELEVANT_ROUTING_KEY, IRRELEVANT_MESSAGE2)

            expected_results = [IRRELEVANT_MESSAGE1, IRRELEVANT_MESSAGE2]
            for index,response in enumerate(self.rabbitmq_client.consume_pending(queue=IRRELEVANT_QUEUE1)):
                expect(response.body).to(equal(expected_results[index]))
        with it('consumes all pending messages'):
            self.rabbitmq_client.publish(IRRELEVANT_EXCHANGE1, IRRELEVANT_ROUTING_KEY, IRRELEVANT_MESSAGE1)
            self.rabbitmq_client.publish(IRRELEVANT_EXCHANGE1, IRRELEVANT_ROUTING_KEY, IRRELEVANT_MESSAGE2)

            expect(self.rabbitmq_client.consume(queue=IRRELEVANT_QUEUE1).body).to(equal(IRRELEVANT_MESSAGE1))
            expect(self.rabbitmq_client.consume(queue=IRRELEVANT_QUEUE1).body).to(equal(IRRELEVANT_MESSAGE2))
            expect(self.rabbitmq_client.consume(queue=IRRELEVANT_QUEUE1)).to(be_none)
            assert_that(self.rabbitmq_client.consume(queue=IRRELEVANT_QUEUE1), is_(None))

        with it('consumes all pending messages consuming next'):
            self.rabbitmq_client.publish(IRRELEVANT_EXCHANGE1, IRRELEVANT_ROUTING_KEY, IRRELEVANT_MESSAGE1)
            self.rabbitmq_client.publish(IRRELEVANT_EXCHANGE1, IRRELEVANT_ROUTING_KEY, IRRELEVANT_MESSAGE2)

            expected_results = [IRRELEVANT_MESSAGE1, IRRELEVANT_MESSAGE2]
            for cont, message in enumerate(self.rabbitmq_client.consume_next(queue=IRRELEVANT_QUEUE1)):
                expect(message.body).to(equal(expected_results[cont]))
                if cont == (len(expected_results) -1):
                    break
