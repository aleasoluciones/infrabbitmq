# -*- coding: utf-8 -*-

from doublex import *
from expects import *
from doublex_expects import *

import os
import logging

from infrabbitmq import rabbitmq, serializers
from infrabbitmq.exceptions import RabbitMQError, RabbitMQNotFoundError


IRRELEVANT_QUEUE1 = 'irrelevant_queue1'
IRRELEVANT_QUEUE2 = 'irrelevant_queue2'
IRRELEVANT_QUEUE3 = 'irrelevant_queue3'

IRRELEVANT_EXCHANGE1 = 'irrelevant_exchange11'
IRRELEVANT_EXCHANGE2 = 'irrelevant_exchange2'
IRRELEVANT_EXCHANGE3 = 'irrelevant_exchange3'


IRRELEVANT_ROUTING_KEY = 'irrelevant_routing_key'
IRRELEVANT_ROUTING_KEY1 = 'irrelevant_routing_key1'
IRRELEVANT_ROUTING_KEY2 = 'irrelevant_routing_key2'
IRRELEVANT_MESSAGE = 'irrelevant_message'
IRRELEVANT_MESSAGE1 = 'irrelevant_message1'
IRRELEVANT_MESSAGE2 = 'irrelevant_message2'


logging.getLogger("pika").setLevel(logging.ERROR)


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

    with context('when publishing more than one direct messages'):
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

        with it('consumes all pending messages consuming next'):
            self.rabbitmq_client.publish(IRRELEVANT_EXCHANGE1, IRRELEVANT_ROUTING_KEY, IRRELEVANT_MESSAGE1)
            self.rabbitmq_client.publish(IRRELEVANT_EXCHANGE1, IRRELEVANT_ROUTING_KEY, IRRELEVANT_MESSAGE2)

            expected_results = [IRRELEVANT_MESSAGE1, IRRELEVANT_MESSAGE2]
            for cont, message in enumerate(self.rabbitmq_client.consume_next(queue=IRRELEVANT_QUEUE1)):
                expect(message.body).to(equal(expected_results[cont]))
                if cont == (len(expected_results) -1):
                    break

    with context('when purging a queue'):
        with it('does NOT consume any message'):
            self.rabbitmq_client.publish(IRRELEVANT_EXCHANGE1, IRRELEVANT_ROUTING_KEY, IRRELEVANT_MESSAGE)

            self.rabbitmq_client.purge(queue=IRRELEVANT_QUEUE1)
            expect(self.rabbitmq_client.consume(queue=IRRELEVANT_QUEUE1)).to(be_none)

    with context('handling errors'):
        with context('when the exchange is not declared'):
            with it('raises RabbitMQNotFoundError'):
                def callback():
                    self.rabbitmq_client.publish('NON_EXISTING_EXCHANGE', 'whatever', IRRELEVANT_MESSAGE)

                expect(callback).to(raise_error(RabbitMQNotFoundError))

        with context('when operating in a not declared queue'):
            with it('raises RabbitMQError'):
                def callback():
                    self.rabbitmq_client.consume(queue='NON_EXISTING_QUEUE')

                expect(callback).to(raise_error(RabbitMQError))

        with context('when trying to do an operation and the connection is not allowed '):
            with it('raises RabbitMQError'):
                broker_uri = 'rabbitmq://WRONGUSER:WRONGPASSWD@localhost:5672/'
                rabbitmq_client = rabbitmq.RabbitMQClient(broker_uri, serializer=serializers.JsonSerializer())
                def callback():
                    rabbitmq_client.exchange_declare(IRRELEVANT_EXCHANGE2, type=rabbitmq.TOPIC)

                expect(callback).to(raise_error(RabbitMQError))


with describe('Rabbitmq client topic specs'):
    with before.each:
        self.broker_uri = os.environ['BROKER_URI']
        self._queue_name = 'test_q_{}'.format(os.getpid())
        self.rabbitmq_client = rabbitmq.RabbitMQClient(self.broker_uri, serializer=serializers.JsonSerializer())
        self.rabbitmq_client.exchange_declare(IRRELEVANT_EXCHANGE2, type=rabbitmq.TOPIC)
        self.rabbitmq_client.queue_declare(queue=self._queue_name, auto_delete=False)

    with after.each:
        self.rabbitmq_client.queue_delete(queue=self._queue_name)
        self.rabbitmq_client.exchange_delete(exchange=IRRELEVANT_EXCHANGE2)

    with context('when topic is #'):
        with it('consumes all messages'):
            _bind_queue_to_topic(self.rabbitmq_client, self._queue_name,'#')

            _publish_on_topic(self.rabbitmq_client, "kernel.critical", IRRELEVANT_MESSAGE1)
            _publish_on_topic(self.rabbitmq_client, "mail.critical.info", IRRELEVANT_MESSAGE2)

            _assert_received_message_is(self.rabbitmq_client, self._queue_name, IRRELEVANT_MESSAGE1)
            _assert_received_message_is(self.rabbitmq_client, self._queue_name, IRRELEVANT_MESSAGE2)

    with context('when topic is kernel.#'):
        with it('consumes kernel messages'):
            _bind_queue_to_topic(self.rabbitmq_client, self._queue_name,'kernel.#')

            _publish_on_topic(self.rabbitmq_client, "kernel.critical", IRRELEVANT_MESSAGE1)
            _publish_on_topic(self.rabbitmq_client, "mail.critical.info", IRRELEVANT_MESSAGE2)

            _assert_received_message_is(self.rabbitmq_client, self._queue_name, IRRELEVANT_MESSAGE1)
            expect(self.rabbitmq_client.consume(queue=self._queue_name)).to(be_none)

def _bind_queue_to_topic(client, queue, topic):
    client.queue_bind(queue=queue, exchange=IRRELEVANT_EXCHANGE2, routing_key=topic)

def _assert_received_message_is(client, queue, message):
    expect(client.consume(queue=queue).body).to(equal(message))

def _publish_on_topic(client, topic, message=IRRELEVANT_MESSAGE):
    client.publish(exchange=IRRELEVANT_EXCHANGE2, routing_key=topic, message=message)

with describe('Rabbitmq client multiple binding specs'):
    with before.each:
        self.broker_uri = os.environ['BROKER_URI']
        self.rabbitmq_client = rabbitmq.RabbitMQClient(self.broker_uri, serializer=serializers.JsonSerializer())
        self.rabbitmq_client.exchange_declare(IRRELEVANT_EXCHANGE3, type=rabbitmq.TOPIC)
        self.rabbitmq_client.queue_declare(queue=IRRELEVANT_QUEUE3, auto_delete=False)

    with after.each:
        self.rabbitmq_client.queue_delete(queue=IRRELEVANT_QUEUE3)
        self.rabbitmq_client.exchange_delete(exchange=IRRELEVANT_EXCHANGE3)

    with context('when receiving from 2 routing keys'):
        with it('consumes all messages'):
            self.rabbitmq_client.queue_bind(queue=IRRELEVANT_QUEUE3, exchange=IRRELEVANT_EXCHANGE3, routing_key=IRRELEVANT_ROUTING_KEY1)
            self.rabbitmq_client.queue_bind(queue=IRRELEVANT_QUEUE3, exchange=IRRELEVANT_EXCHANGE3, routing_key=IRRELEVANT_ROUTING_KEY2)

            self.rabbitmq_client.publish(exchange=IRRELEVANT_EXCHANGE3, routing_key=IRRELEVANT_ROUTING_KEY1, message=IRRELEVANT_MESSAGE1)
            self.rabbitmq_client.publish(exchange=IRRELEVANT_EXCHANGE3, routing_key=IRRELEVANT_ROUTING_KEY2, message=IRRELEVANT_MESSAGE2)

            expect(self.rabbitmq_client.consume(queue=IRRELEVANT_QUEUE3).body).to(equal(IRRELEVANT_MESSAGE1))
            expect(self.rabbitmq_client.consume(queue=IRRELEVANT_QUEUE3).body).to(equal(IRRELEVANT_MESSAGE2))
