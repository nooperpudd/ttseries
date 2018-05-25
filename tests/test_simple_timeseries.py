# encoding:utf-8
import datetime
import unittest

import redis

from ttseries import RedisSampleTimeSeries
from .mixin import Mixin


class RedisSimpleTSTest(unittest.TestCase, Mixin):

    def setUp(self):
        redis_client = redis.StrictRedis()
        self.time_series = RedisSampleTimeSeries(redis_client, max_length=10)
        self.timestamp = datetime.datetime.now().timestamp()

    def tearDown(self):
        self.time_series.flush()

    def test_delete_key(self):
        key = "APPL:SECOND:5"
        data = {"value": 10.4}
        self.time_series.add(key, self.timestamp, data)
        self.time_series.delete(key)

        self.assertFalse(self.time_series.exists(key))

    def test_remove_many(self):

        data_list = self.generate_data(10)
        keys = self.prepare_many_data(data_list)

        remove_keys = keys[:5]
        rest_keys = keys[5:]

        self.time_series.remove_many(remove_keys)

        for key in remove_keys:
            self.assertFalse(self.time_series.exists(key))

        for key in rest_keys:
            result = self.time_series.get_slice(key)
            self.assertListEqual(data_list, result)

