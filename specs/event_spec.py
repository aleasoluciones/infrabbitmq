# -*- coding: utf-8 -*-

from mamba import description, context, it
from expects import expect, equal

from infrabbitmq import events


with description('Events'):
    with context('when a topic prefix is set'):
        with it('returns the topic with the prefix'):
            event = events.Event('name', 'network', 'data', topic_prefix='prefix')

            expect(event.topic).to(equal('network.prefix.name'))

    with context('when a topic prefix is not set'):
        with it('returns the topic without the prefix'):
            event = events.Event('name', 'network', 'data')

            expect(event.topic).to(equal('network.name'))
