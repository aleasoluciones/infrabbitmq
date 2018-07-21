# -*- coding: utf-8 -*-

import pika
import time
from functools import wraps

from infrabbitmq.exceptions import ChannelClosedError, RabbitMQError, RabbitMQClientError, EmptyQueueError


class ClientWrapper(object):

    def __init__(self, client=pika):
        self._client = client
        self._connection = None
        self._channel = None
        self._closing = False
        self._broker_uri = None

    def raise_client_error(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except pika.exceptions.AMQPError as exc:
                raise RabbitMQClientError(exc)
        return wrapper

    @raise_client_error
    def connect(self, broker_uri):
        self.broker_uri = broker_uri
        self._connection = pika.BlockingConnection(pika.URLParameters(self.broker_uri))
        self._channel = self._connection.channel()
        self._channel.confirm_delivery()

    @raise_client_error
    def exchange_declare(self, exchange, type, **kwargs):
        self._channel.exchange_declare(exchange=exchange,
                                       exchange_type=type,
                                       passive=kwargs.get('passive', False),
                                       durable=kwargs.get('durable', False),
                                       auto_delete=kwargs.get('auto_delete', False),
                                       internal=kwargs.get('internal', False),
                                       arguments=kwargs.get('arguments', {}))

    @raise_client_error
    def queue_declare(self, queue, auto_delete=True, exclusive=False, durable=False, arguments=None):
        self._channel.queue_declare(queue,
                                    durable=durable,
                                    exclusive=exclusive,
                                    auto_delete=auto_delete,
                                    arguments=arguments)

    @raise_client_error
    def basic_publish(self, exchange, routing_key, body, **kwargs):
        properties = pika.spec.BasicProperties(headers=kwargs.get('headers', {}))
        try:
            self._channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body, properties=properties, mandatory=True)
        except pika.exceptions.ChannelClosed:
            raise ChannelClosedError


    @raise_client_error
    def start_consume(self, queue, timeout):
        msg_body = {}
        try:
            for method_frame, properties, body in self._channel.consume(queue, inactivity_timeout=timeout):
                msg_body['body'] = body.decode('utf-8')
                self._channel.basic_ack(method_frame.delivery_tag)
                break
        except pika.exceptions.ChannelClosed:
            raise ChannelClosedError
        except pika.exceptions.AMQPError as exc:
            raise RabbitMQError(exc)
        except TypeError:
            raise EmptyQueueError
        return msg_body

    @raise_client_error
    def queue_bind(self, queue, exchange, routing_key=''):
        self._channel.queue_bind(queue=queue, exchange=exchange, routing_key=routing_key)

    @raise_client_error
    def queue_delete(self, queue):
        self._channel.queue_delete(queue)

    @raise_client_error
    def queue_purge(self, queue):
        self._channel.queue_purge(queue)

    @raise_client_error
    def exchange_delete(self, exchange):
        self._channel.exchange_delete(exchange)

    @raise_client_error
    def disconnect(self):
        self._channel.close()
        self._connection.close()

