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

    def test_add_many(self):

        data_list = self.generate_data(10)
        key = self.add_data_list(data_list)

        self.time_series.add_many(key, data_list)
        results = self.time_series.get_slice(key)
        self.assertListEqual(data_list, results)

    def test_add_many_max_length(self):

        data_list = self.generate_data(20)
        key = self.add_data_list(data_list)

        self.time_series.add_many(key, data_list)
        results = self.time_series.get_slice(key)

        filter_data = data_list[10:]
        removed_data = data_list[:10]
        self.assertListEqual(results, filter_data)
        start_timestamp = removed_data[0][0]
        end_timestamp = removed_data[-1][0]
        self.assertEqual(self.time_series.count(key, start_timestamp,
                                                end_timestamp), 0)

    def test_add_max_length_complete(self):

        data_list = self.generate_data(40)
        key = self.add_data_list(data_list)
        data_list1 = data_list[:20]
        data_list2 = data_list[20:40]

        # add data list 1
        self.time_series.add_many(key, data_list1)
        filter_data1 = data_list1[10:]

        results1 = self.time_series.get_slice(key)
        self.assertListEqual(results1, filter_data1)

        self.time_series.add_many(key,data_list2)

        filter_data2 = data_list2[10:]
        results2 = self.time_series.get_slice(key)
        self.assertListEqual(results2, filter_data2)


