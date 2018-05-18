# encoding:utf-8
import abc
import datetime
import decimal

from dateutil import parser

import msgpack


class BaseSerializer(abc.ABC):
    """
    The base serializer class,
    only defines the signature for loads and dumps
    """
    @abc.abstractmethod
    def loads(self, data, *args, **kwargs):
        """
        Deserialize the data
        :param data: the structure data need to be
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def dumps(self, data, *args, **kwargs):
        """
        Serialize ``data`` to kinds of type
        :param data:
        """
        raise NotImplementedError()


class MsgPackDecoder(object):
    """
    decode serializer data
    """

    def decode(self, obj):
        """
        :param obj:
        :return:obj
        """
        if "__cls__" in obj:
            decode_func = getattr(self, "decode_%s" % obj["__cls__"])
            return decode_func(obj)
        return obj

    def decode_datetime(self, obj):
        return parser.parse(obj["str"])

    def decode_date(self, obj):
        return parser.parse(obj["str"]).date()

    def decode_time(self, obj):
        return parser.parse(obj["str"]).time()

    def decode_decimal(self, obj):
        return decimal.Decimal(obj["str"])


class MsgPackEncoder(object):
    """
    encode the data type to the message pack format
    """
    def encode(self, obj):
        """
        :param obj:
        :return: dict
        """
        if type(obj) is datetime.date:
            return {"__cls__": "date", "str": obj.isoformat()}
        elif type(obj) is datetime.datetime:
            return {"__cls__": "datetime", "str": obj.isoformat()}
        elif type(obj) is datetime.time:
            return {"__cls__": "time", "str": obj.isoformat()}
        elif isinstance(obj, decimal.Decimal):
            return {"__cls__": "decimal", "str": str(obj)}
        else:
            return obj


class MsgPackSerializer(BaseSerializer):
    """
    MessagePack serializer

    CPython’s GC starts when growing allocated object.
    This means unpacking may cause useless GC.
    You can use gc.disable() when unpacking large message.
    """

    def loads(self, data, *args, **kwargs):
        """
        deserializer data from message-pack format
        :param data: bytes
        :return:obj
        """
        return msgpack.unpackb(data, encoding="utf-8", object_hook=MsgPackDecoder().decode, **kwargs)

    def dumps(self, data, *args, **kwargs):
        """
        serializer data to message-pack format
        :param data: obj
        :return: bytes
        """
        return msgpack.packb(data, encoding="utf-8", default=MsgPackEncoder().encode, **kwargs)
