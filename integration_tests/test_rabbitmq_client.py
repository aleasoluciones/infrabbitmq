# -*- coding: utf-8 -*-

import unittest
from hamcrest import *
from doublex import *
import os

from infrabbitmq import rabbitmq, serializers

IRRELEVANT_QUEUE1 = 'irrelevant_queue1'
IRRELEVANT_QUEUE2 = 'irrelevant_queue2'
IRRELEVANT_QUEUE3 = 'irrelevant_queue3'

IRRELEVANT_EXCHANGE1 = 'irrelevant_exchange1'
IRRELEVANT_EXCHANGE2  = 'irrelevant_exchange2'
IRRELEVANT_EXCHANGE3 = 'irrelevant_exchange3'


IRRELEVANT_ROUTING_KEY = 'irrelevant_routing_key'
IRRELEVANT_ROUTING_KEY1 = 'irrelevant_routing_key1'
IRRELEVANT_ROUTING_KEY2 = 'irrelevant_routing_key2'
IRRELEVANT_MESSAGE = 'irrelevant_message'
IRRELEVANT_MESSAGE1 = 'irrelevant_message1'
IRRELEVANT_MESSAGE2 = 'irrelevant_message2'


class RabbitMQClientTest(unittest.TestCase):

    def setUp(self):
        self.broker_uri = os.environ['BROKER_URI']
        self.rabbitmq_client = rabbitmq.RabbitMQClient(self.broker_uri, serializer=serializers.JsonSerializer())
        self.rabbitmq_client.exchange_declare(exchange=IRRELEVANT_EXCHANGE1, type=rabbitmq.DIRECT)
        self.rabbitmq_client.queue_declare(IRRELEVANT_QUEUE1, auto_delete=False)
        self.rabbitmq_client.queue_bind(IRRELEVANT_QUEUE1, IRRELEVANT_EXCHANGE1, routing_key=IRRELEVANT_ROUTING_KEY)

    def tearDown(self):
        self.rabbitmq_client.queue_delete(queue=IRRELEVANT_QUEUE1)
        self.rabbitmq_client.exchange_delete(exchange=IRRELEVANT_EXCHANGE1)


    def test_purge(self):
        self.rabbitmq_client.publish(IRRELEVANT_EXCHANGE1, IRRELEVANT_ROUTING_KEY, IRRELEVANT_MESSAGE)

        self.rabbitmq_client.purge(queue=IRRELEVANT_QUEUE1)
        assert_that(self.rabbitmq_client.consume(queue=IRRELEVANT_QUEUE1), is_(None))


class RabbitMQClientTopicsTest(unittest.TestCase):

    def setUp(self):
        self.broker_uri = os.environ['BROKER_URI']
        self.rabbitmq_client = rabbitmq.RabbitMQClient(self.broker_uri, serializer=serializers.JsonSerializer())
        self.rabbitmq_client.exchange_declare(IRRELEVANT_EXCHANGE2, type=rabbitmq.TOPIC)
        self.rabbitmq_client.queue_declare(queue=self._queue_name, auto_delete=False)

    def tearDown(self):
        self.rabbitmq_client.queue_delete(queue=self._queue_name)
        self.rabbitmq_client.exchange_delete(exchange=IRRELEVANT_EXCHANGE2)

    def test_consume_all_messages(self):
        self._bind_queue_to_topic('#')

        self._publish_on_topic("kern.critical", IRRELEVANT_MESSAGE1)
        self._publish_on_topic("mail.critical.info", IRRELEVANT_MESSAGE2)

        self._assert_received_message_is(IRRELEVANT_MESSAGE1)
        self._assert_received_message_is(IRRELEVANT_MESSAGE2)

    def test_consume_all_kernel_messages(self):
        self._bind_queue_to_topic('kern.#')

        self._publish_on_topic("kern.critical", IRRELEVANT_MESSAGE1)
        self._publish_on_topic("mail.critical.info", IRRELEVANT_MESSAGE2)

        self._assert_received_message_is(IRRELEVANT_MESSAGE1)
        assert_that(self.rabbitmq_client.consume(queue=self._queue_name), is_(None))

    def _bind_queue_to_topic(self, topic):
        self.rabbitmq_client.queue_bind(queue=self._queue_name, exchange=IRRELEVANT_EXCHANGE2, routing_key=topic)

    def _publish_on_topic(self, topic, message=IRRELEVANT_MESSAGE):
        self.rabbitmq_client.publish(exchange=IRRELEVANT_EXCHANGE2, routing_key=topic, message=message)

    def _assert_received_message_is(self, message):
        assert_that(self.rabbitmq_client.consume(queue=self._queue_name).body, is_(message))

    @property
    def _queue_name(self):
        return "test_q_%i" % os.getpid()


class RabbitMQClientMultipleBindingsTestX(unittest.TestCase):

    def setUp(self):
        self.broker_uri = os.environ['BROKER_URI']
        self.rabbitmq_client = rabbitmq.RabbitMQClient(self.broker_uri, serializer=serializers.JsonSerializer())
        self.rabbitmq_client.exchange_declare(exchange=IRRELEVANT_EXCHANGE3, type=rabbitmq.TOPIC)
        self.rabbitmq_client.queue_declare(queue=IRRELEVANT_QUEUE3, auto_delete=False)

    def tearDown(self):
        self.rabbitmq_client.queue_delete(queue=IRRELEVANT_QUEUE3)
        self.rabbitmq_client.exchange_delete(exchange=IRRELEVANT_EXCHANGE3)

    def test_receive_from_two_routing_keys(self):
        self.rabbitmq_client.queue_bind(queue=IRRELEVANT_QUEUE3, exchange=IRRELEVANT_EXCHANGE3, routing_key=IRRELEVANT_ROUTING_KEY1)
        self.rabbitmq_client.queue_bind(queue=IRRELEVANT_QUEUE3, exchange=IRRELEVANT_EXCHANGE3, routing_key=IRRELEVANT_ROUTING_KEY2)

        self.rabbitmq_client.publish(exchange=IRRELEVANT_EXCHANGE3, routing_key=IRRELEVANT_ROUTING_KEY1, message=IRRELEVANT_MESSAGE1)
        self.rabbitmq_client.publish(exchange=IRRELEVANT_EXCHANGE3, routing_key=IRRELEVANT_ROUTING_KEY2, message=IRRELEVANT_MESSAGE2)

        assert_that(self.rabbitmq_client.consume(queue=IRRELEVANT_QUEUE3).body, is_(IRRELEVANT_MESSAGE1))
        assert_that(self.rabbitmq_client.consume(queue=IRRELEVANT_QUEUE3).body, is_(IRRELEVANT_MESSAGE2))


class RabbitMQClientErrorTestX(unittest.TestCase):

    def setUp(self):
        self.broker_uri = os.environ['BROKER_URI']
        self.rabbitmq_client = rabbitmq.RabbitMQClient(self.broker_uri, serializer=serializers.JsonSerializer())

    def test_raise_error_when_publish_to_undeclared_exchange(self):
        self.assertRaises(rabbitmq.RabbitMQNotFoundError,
            self.rabbitmq_client.publish,
            exchange='NON_EXISTING_EXCHANGE', routing_key='whatever', message=IRRELEVANT_MESSAGE)

    def test_raise_error_when_operations_on_undeclared_queue(self):
        self.assertRaises(rabbitmq.RabbitMQError, self.rabbitmq_client.consume, queue='NON_EXISTING_QUEUE')
        self.assertRaises(rabbitmq.RabbitMQError, self.rabbitmq_client.purge, queue='NON_EXISTING_QUEUE')

    def test_raise_error_trying_to_declare_a_exchange_and_the_connection_is_not_allowed(self):
        self.broker_uri = 'rabbitmq://WRONGUSER:WRONGPASSWD@localhost:5672/'
        self.rabbitmq_client = rabbitmq.RabbitMQClient(self.broker_uri, serializer=serializers.JsonSerializer())
        self.assertRaises(rabbitmq.RabbitMQError,
                self.rabbitmq_client.exchange_declare,
                exchange=IRRELEVANT_EXCHANGE1, type=rabbitmq.DIRECT)


