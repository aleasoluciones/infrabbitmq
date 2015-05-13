# -*- coding: utf-8 -*-

import json
import datetime

from hamcrest import *
from doublex import *

from infrabbitmq import jsonserializer, events

IRRELEVANT_DATA = {
    'name': 'event name',
    'data': 'event data',
    'network': 'event network',
}

class TestJsonSerializer(object):
    def setUp(self):
        self.serializer = jsonserializer.JsonSerializer()

    def test_serializes_to_json(self):
        serialized = self.serializer.dumps(IRRELEVANT_DATA)

        assert_that(json.loads(serialized), is_(IRRELEVANT_DATA))

    def test_deserializes_datetime(self):

        t = datetime.datetime(2015,5,13,12,50,19)
        t_timestamp_format = 1431521419

        serialized = self.serializer.dumps(t)

        assert_that(json.loads(serialized), equal_to(t_timestamp_format))
