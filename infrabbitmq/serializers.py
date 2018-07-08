# -*- coding: utf-8 -*-

import json
import datetime
import pickle


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


class PickleSerializer(object):
    def loads(self, serialized_object, **kwargs):
        return pickle.loads(serialized_object, **kwargs)

    def dumps(self, obj, **kwargs):
        return pickle.dumps(obj, **kwargs)


class JsonOrPickleSerializer(object):
    def __init__(self):
        self._serializers =  [JsonSerializer(), PickleSerializer()]

    def loads(self, data):
        for serializer in self._serializers:
            try:
                return serializer.loads(data)
            except Exception as exc:
                pass

        raise DeserializeError(exc, data)

    def dumps(self, obj):
        raise NotImplementedError('This serializer should not be used to serialize, only deserialize')


class DeserializeError(Exception):
    def __init__(self, exception, data):
        super(DeserializeError, self).__init__(self, exception, data)
        self.exception = exception
        self.data = data
