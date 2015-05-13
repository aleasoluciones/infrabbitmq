# -*- coding: utf-8 -*-

import json
import datetime

class JsonSerializer(object):

    def loads(self, serialized_object, **kwargs):
        return json.loads(serialized_object, **kwargs)

    def dumps(self, obj, **kwargs):
        try:
            data = obj.__dict__
        except AttributeError:
            data = obj

        return json.dumps(data, default=_json_serializer, **kwargs)

def _json_serializer(obj):
    if isinstance(obj, datetime.datetime):
        return (obj - datetime.datetime(1970, 1, 1)).total_seconds()
    return json.JSONEncoder().default(obj)
