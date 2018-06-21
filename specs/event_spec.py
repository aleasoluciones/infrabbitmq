# -*- coding: utf-8 -*-

from expects import expect, equal

from infrabbitmq import events

PREFIX = 'irrelevant-prefix'
NETWORK = 'irrelevant-network'
NAME = 'irrelevant-name'

with description('Event test'):
    with context('when creating an event'):
        with context('when specifing a prefix'):
            with it('returns the topic with the specified prefix'):
                event = events.Event(NAME, NETWORK, 'data', topic_prefix=PREFIX)

                expected_topic = '{}.{}.{}'.format(NETWORK, PREFIX, NAME)
                expect(event.topic).to(equal(expected_topic))

        with context('when NOT specifing a prefix'):
            with it('returns the topic without  prefix'):
                event = events.Event(NAME, NETWORK, 'data')

                expected_topic = '{}.{}'.format(NETWORK, NAME)
                expect(event.topic).to(equal(expected_topic))
