# -*- coding: utf-8 -*-

import pika
import time


class ClientWrapper(object):

    def __init__(self, client=pika):
        self._client = client
        self._connection = None
        self._channel = None
        self._closing = False
        self._broker_uri = None

    def connect(self, broker_uri):
        self.broker_uri = broker_uri
        self._connection = pika.BlockingConnection(pika.URLParameters(self.broker_uri))
        self._channel = self._connection.channel()
        self._channel.confirm_delivery()

    def exchange_declare(self, exchange, type, **kwargs):
        self._channel.exchange_declare(exchange, type, kwargs)

    def queue_declare(self, queue, auto_delete=True, exclusive=False, durable=False, arguments=None):
        self._channel.queue_declare(queue)

    def basic_publish(self, exchange, routing_key, body, **kwargs):
        self._channel.basic_publish(exchange, routing_key, body, mandatory=True)

    def start_consume(self, queue, timeout):
        msg_body = {}
        for method_frame, properties, body in self._channel.consume(queue, inactivity_timeout=timeout):
            msg_body['body'] = body.decode('utf-8')
            self._channel.basic_ack(method_frame.delivery_tag)
            break
        return msg_body

    def queue_bind(self, queue, exchange, routing_key=''):
        self._channel.queue_bind(queue=queue, exchange=exchange, routing_key=routing_key)

    def queue_delete(self, queue):
        self._channel.queue_delete(queue)

    def queue_purge(self, queue):
        self._channel.queue_purge(queue)

    def exchange_delete(self, exchange):
        self._channel.exchange_delete(exchange)

    def disconnect(self):
        self._channel.close()
        self._connection.close()

    def _on_connection_closed(self):
        print(">>>> on connection close")


