# encoding:utf-8
import datetime
import decimal
import unittest

import pytz

from ttseries import serializers


class MsgPackSerializersTests(unittest.TestCase):
    def setUp(self):
        self.msgpack_serializer = serializers.MsgPackSerializer()

    def test_loads_and_dumps(self):
        obj = {"aaa": ["bac", {"dd": "e"}, "11", 11.3, 11]}
        encode_data = self.msgpack_serializer.dumps(obj)
        decode_data = self.msgpack_serializer.loads(encode_data)
        self.assertDictEqual(obj, decode_data)

    def test_decimal(self):
        obj = {"data": decimal.Decimal("1.2444494")}
        encode_data = self.msgpack_serializer.dumps(obj)
        decode_data = self.msgpack_serializer.loads(encode_data)
        self.assertDictEqual(obj, decode_data)

    def test_datetime(self):
        obj = {"data": datetime.datetime.now()}
        encode_data = self.msgpack_serializer.dumps(obj)
        decode_data = self.msgpack_serializer.loads(encode_data)
        self.assertDictEqual(obj, decode_data)

    def test_date(self):
        obj = {"data": datetime.date.today()}
        encode_data = self.msgpack_serializer.dumps(obj)
        decode_data = self.msgpack_serializer.loads(encode_data)
        self.assertDictEqual(obj, decode_data)

    def test_time(self):
        obj = {"data": datetime.datetime.now().time()}
        encode_data = self.msgpack_serializer.dumps(obj)
        decode_data = self.msgpack_serializer.loads(encode_data)
        self.assertDictEqual(obj, decode_data)

    def test_datetime_with_timezone(self):
        obj = {"data": datetime.datetime.now(tz=pytz.timezone("Asia/Shanghai"))}
        encode_data = self.msgpack_serializer.dumps(obj)
        decode_data = self.msgpack_serializer.loads(encode_data)
        self.assertEqual(obj["data"], decode_data["data"])
        self.assertDictEqual(obj, decode_data)

    def test_float(self):
        obj = {"data": 13.4}
        encode_data = self.msgpack_serializer.dumps(obj)
        decode_data = self.msgpack_serializer.loads(encode_data)
        self.assertDictEqual(obj, decode_data)

    def test_timestamp(self):
        obj = {"data": datetime.datetime.now().timestamp()}
        encode_data = self.msgpack_serializer.dumps(obj)
        decode_data = self.msgpack_serializer.loads(encode_data)
        self.assertDictEqual(obj, decode_data)

    def test_unicode(self):
        obj = {"data": "你好"}
        encode_data = self.msgpack_serializer.dumps(obj)
        decode_data = self.msgpack_serializer.loads(encode_data)
        self.assertDictEqual(obj, decode_data)
