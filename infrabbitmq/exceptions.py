# -*- coding: utf-8 -*-

class RabbitMQError(Exception):
    pass


class RabbitMQNotFoundError(RabbitMQError):
    pass

class ChannelClosedError(RabbitMQError):
    pass

class RabbitMQClientError(RabbitMQError):
    pass

class EmptyQueueError(RabbitMQError):
    pass

