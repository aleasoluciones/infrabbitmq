2017-01-11
==========
* Internal: use Python 3 compatible print()

2016-08-16
==========
* Update infcommon dependency version

2016-05-30
==========
* Internal: use CRITICAL logging level instead of ERROR for Sentry and change Sentry level to CRITICAL.

2015-12-30
==========
* Allow to pass a custom "event builder" to event processors so we can handle events without the {'name': 'event_name', ...} format.
