# -*- coding: utf-8 -*-

import json
import datetime

from expects import expect, equal

from infrabbitmq import serializers, events

IRRELEVANT_DATA = {
    'name': 'event name',
    'data': 'event data',
    'network': 'event network',
}

with description('Json serializer specs'):
    with before.each:
        self.serializer = serializers.JsonSerializer()

    with context('when serializing to json'):
        with it('deserializes correctly'):
            serialized = self.serializer.dumps(IRRELEVANT_DATA)

            expect(json.loads(serialized)).to(equal(IRRELEVANT_DATA))

    with context('when serializing datatime'):
        with it('deserializes correctly'):
            t = datetime.datetime(2015,5,13,12,50,19)
            t_timestamp_format = 1431521419

            serialized = self.serializer.dumps(t)

            expect(json.loads(serialized)).to(equal(t_timestamp_format))


