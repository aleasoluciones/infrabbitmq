# -*- coding: utf-8 -*-

import logging
from functools import wraps
import socket, select, errno
from infrabbitmq import events, client_wrapper
from infrabbitmq.exceptions import (RabbitMQError,
                                    RabbitMQNotFoundError,
                                    ChannelClosedError,
                                    RabbitMQClientError,
                                    EmptyQueueError)

DIRECT = 'direct'
TOPIC = 'topic'
X_DELAYED = 'x-delayed-message'

# AMQP topics
# * (star) can substitute for exactly one word.
# # (hash) can substitute for zero or more words.


class RabbitMQClient(object):

    def __init__(self, broker_uri, serializer):
        self.broker_uri = broker_uri.replace('rabbitmq', 'amqp')
        self._client = None
        self.serializer = serializer

    @property
    def client(self):
        if self._client is None:
            self._connect()
        return self._client

    def raise_rabbitmq_error(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except ChannelClosedError as exc:
                self._client = None
                raise RabbitMQNotFoundError(exc)
            except (socket.error, RabbitMQClientError) as exc:
                logging.info("Reconnecting, Error rabbitmq %s %s" % (type(exc), exc), exc_info=True)
                self._client = None
                raise RabbitMQError(exc)
        return wrapper

    @raise_rabbitmq_error
    def publish(self, exchange, routing_key, message, **kwargs):
        self.client.basic_publish(exchange=exchange, routing_key=routing_key,
                    body=self._serialize(message), **kwargs)

    @raise_rabbitmq_error
    def exchange_declare(self, exchange, type, **kwargs):
        if type == X_DELAYED:
            kwargs['arguments'] = {'x-delayed-type': 'topic'}
        self.client.exchange_declare(exchange=exchange, type=type, **kwargs)

    @raise_rabbitmq_error
    def exchange_delete(self, exchange):
        self.client.exchange_delete(exchange=exchange)

    @raise_rabbitmq_error
    def queue_declare(self, queue, auto_delete=True, exclusive=False, durable=False, message_ttl=None):
        arguments = {}
        if message_ttl is not None:
            arguments['x-message-ttl'] = message_ttl
        self.client.queue_declare(queue=queue, auto_delete=auto_delete, exclusive=exclusive, durable=durable, arguments=arguments)

    @raise_rabbitmq_error
    def queue_bind(self, queue, exchange, routing_key=''):
        self.client.queue_bind(queue=queue, exchange=exchange, routing_key=routing_key)

    @raise_rabbitmq_error
    def queue_unbind(self, queue, exchange, routing_key=''):
        self.client.queue_unbind(queue=queue, exchange=exchange, routing_key=routing_key)

    @raise_rabbitmq_error
    def queue_delete(self, queue):
        self.client.queue_delete(queue=queue)

    @raise_rabbitmq_error
    def consume(self, queue, timeout=1):
        try:
            message = self.client.start_consume(queue=queue, timeout=timeout)
            message['body'] = self._deserialize(message['body'])
            message = RabbitMQMessage(message)
        except EmptyQueueError:
            message = None
        self.disconnect()
        return message

    def _consume_cancel_and_disconnect(self, consume_promise):
        self.disconnect()

    def _basic_cancel(self, consume_promise):
        self.client.wait(self.client.basic_cancel(consume_promise))

    @raise_rabbitmq_error
    def purge(self, queue):
        self.client.queue_purge(queue)

    def _connect(self):
        self._client = client_wrapper.ClientWrapper(self.broker_uri)
        self._client.connect(self.broker_uri)

    @raise_rabbitmq_error
    def connect(self):
        if self.client:
            self.disconnect()

    @raise_rabbitmq_error
    def disconnect(self):
        try:
            self.client.disconnect()
        except Exception:
            pass
        finally:
            self._client = None

    def _serialize(self, value):
        return self.serializer.dumps(value)

    def _deserialize(self, value):
        return self.serializer.loads(value)

    def consume_next(self, queue, timeout=1):
        try:
            while True:
                try:
                    message = self.client.start_consume(queue=queue, timeout=timeout)
                    if message:
                        message['body'] = self._deserialize(message['body'])
                        yield RabbitMQMessage(message)
                    else:
                        yield None
                except select.error as exc:
                    # http://stackoverflow.com/questions/5633067/signal-handling-in-pylons
                    if exc[0] != errno.EINTR:
                        logging.info("Interrupted System Call")
        except (ChannelClosedError) as exc:
            raise RabbitMQNotFoundError(exc)
        except (socket.error, RabbitMQClientError) as exc:
            logging.critical("Reconnecting, Error rabbitmq %s %s" % (type(exc), exc), exc_info=True)
            self._client = None
            raise RabbitMQError(exc)
        except EmptyQueueError:
            yield None


    @raise_rabbitmq_error
    def consume_pending(self, queue, timeout=1):
        return RabbitMQQueueIterator(queue, self.client, timeout, self._deserialize)


class RabbitMQQueueIterator(object):

    def __init__(self, queue, client, timeout, deserialize_func):
        self.queue = queue
        self.client = client
        self.timeout = timeout
        self.deserialize_func = deserialize_func

    def __iter__(self):
        return self

    def next(self):
        return self.__next__()

    def __next__(self):
        try:
            message = self.client.start_consume(queue=self.queue, timeout=self.timeout)
        except EmptyQueueError:
            raise StopIteration()
        try:
            message['body'] = self.deserialize_func(message['body'])
            return RabbitMQMessage(message)
        except Exception as exc:
            logging.critical("Error consuming from %s %s %s" % (self.queue, type(exc), exc), exc_info=True)
            return self.__next__()


class RabbitMQMessage(object):

    def __init__(self, message):
        self.message = message

    @property
    def correlation_id(self):
        return self.message['headers'].get('correlation_id')

    @property
    def reply_to(self):
        return self.message['headers'].get('reply_to')

    @property
    def host(self):
        return self.message['headers'].get('HOST')

    @property
    def body(self):
        return self.message['body']

    @property
    def routing_key(self):
        return self.message.get('routing_key')

    def __str__(self):
        return str(self.body)


class RabbitMQQueueEventProcessor(object):

    def __init__(self, queue_name, processor, rabbitmq_client, exchange, topics, exchange_options, queue_options, event_builder, exchange_type=TOPIC):
        self.queue_name = queue_name
        self.processor = processor
        self.rabbitmq_client = rabbitmq_client
        self.topics = topics
        self.exchange = exchange
        self.exchange_options = exchange_options
        self.queue_options = queue_options
        self.event_builder = event_builder
        self.exchange_type = exchange_type
        if len(self.queue_name) > 0:
            self._declare_recurses()

    def _declare_recurses(self):
        self._declare_exchange()
        self._declare_queue()
        self._bind_queue_to_topics()

    def _connection_setup(self):
        self.rabbitmq_client.disconnect()
        self._declare_recurses()


    def _declare_exchange(self):
        arguments = {'x-delayed-type': 'topic'} if self.exchange_type is X_DELAYED else {}
        self.rabbitmq_client.exchange_declare(self.exchange,
                                              self.exchange_type,
                                              durable=self.exchange_options.get('durable', True),
                                              auto_delete=self.exchange_options.get('auto_delete', False),
                                              arguments=arguments)

    def _declare_queue(self):
        self.rabbitmq_client.queue_declare(queue=self.queue_name,
                                           durable=self.queue_options.get('durable', True),
                                           auto_delete=self.queue_options.get('auto_delete', False),
                                           message_ttl=self.queue_options.get('message_ttl'))

    def _bind_queue_to_topics(self):
        for topic in self.topics:
            self.rabbitmq_client.queue_bind(queue=self.queue_name,
                                            exchange=self.exchange,
                                            routing_key=topic)

    def process_body(self, max_iterations=None):
        self._connection_setup()
        while True:
            for index, message in enumerate(self.rabbitmq_client.consume_next(queue=self.queue_name, timeout=1)):
                if message is not None:
                    try:
                        self.processor.process(self.event_builder(message.body))
                    except Exception:
                        logging.critical("Error processing {message} with exception".format(message=message.body), exc_info=True)
                if max_iterations is not None and index >= (max_iterations - 1):
                    return


class EventPublisher(object):
    def __init__(self, rabbitmq_client, clock, exchange):
        self.exchange = exchange
        self.rabbitmq_client = rabbitmq_client
        self.clock = clock

    def publish(self, event_name, network, data=None, id=None, topic_prefix=None):
        now = self.clock.now()
        event = events.Event(event_name, network, data, self.clock.timestamp(now), id, topic_prefix, timestamp_str=str(now))
        self.publish_event_object(event)

    def publish_event_object(self, event):
        now = self.clock.now()
        event.timestamp = self.clock.timestamp(now)
        self.rabbitmq_client.exchange_declare(exchange=self.exchange, type=TOPIC, durable=True)
        self.rabbitmq_client.publish(exchange=self.exchange,
                                     routing_key=event.topic,
                                     message=event)

    def publish_with_ttl(self, event_name, network, ttl, data=None, id=None, topic_prefix=None):
        message_header = {'expiration': str(ttl)}
        now = self.clock.now()
        event = events.Event(event_name, network, data, self.clock.timestamp(now), id, topic_prefix, timestamp_str=str(now))
        self.rabbitmq_client.exchange_declare(exchange=self.exchange, type=TOPIC, durable=True)
        self.rabbitmq_client.publish(exchange=self.exchange,
                                     routing_key=event.topic,
                                     message=event,
                                     headers=message_header)

    def publish_with_delay(self, event_name, network, delay=0, data=None, id=None, topic_prefix=None):
        message_header = {'x-delay': str(delay)}
        now = self.clock.now()
        event = events.Event(event_name, network, data, self.clock.timestamp(now), id, topic_prefix, timestamp_str=str(now))
        self.rabbitmq_client.exchange_declare(exchange=self.exchange, type=X_DELAYED, durable=True, arguments={'x-delayed-type': 'topic'})
        self.rabbitmq_client.publish(exchange=self.exchange,
                                     routing_key=event.topic,
                                     message=event,
                                     headers=message_header)
