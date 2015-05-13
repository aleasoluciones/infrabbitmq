# -*- coding: utf-8 -*-

import unittest
from hamcrest import *

from infrabbitmq import events


class EventTest(unittest.TestCase):

    def test_returns_topic_with_the_specified_prefix(self):
        event = events.Event('name', 'network', 'data', topic_prefix='prefix')

        assert_that(event.topic, is_('network.prefix.name'))

    def test_returns_topic_without_prefix(self):
        event = events.Event('name', 'network', 'data')

        assert_that(event.topic, is_('network.name'))
