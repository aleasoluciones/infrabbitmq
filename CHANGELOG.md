2019-07-22
==========
* [Internal] Use serializer from infcommon instead local serializers


2018-05-29
==========
* [Internal] Update factory with a new function to create event_publisher_json_serializer

2018-05-11
==========
* publish_with_delay method added
* exchange declaration allows x-delayed-message

2018-01-15
==========
* Internal: Updated setup_venv.sh to upgrade always

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
