# encoding:utf-8
import datetime
import unittest

import redis

from ttseries import RedisHashTimeSeries
from .mixin import Mixin


class RedisHashTSTest(unittest.TestCase, Mixin):

    def setUp(self):
        redis_client = redis.StrictRedis()
        self.time_series = RedisHashTimeSeries(redis_client, max_length=10)

        self.timestamp = datetime.datetime.now().timestamp()

    def tearDown(self):
        self.time_series.flush()

    # **************** add func ***************

    def test_add_repeated_values(self):
        """
        ensure once insert repeated data
        """
        key = "APPL:SECOND:2"
        timestamp2 = self.timestamp + 1
        data = {"value": 1}

        result1 = self.time_series.add(key, self.timestamp, data)
        result2 = self.time_series.add(key, timestamp2, data)

        result_data1 = self.time_series.get(key, self.timestamp)
        result_data2 = self.time_series.get(key, timestamp2)

        self.assertTrue(result1)
        self.assertTrue(result2)
        self.assertDictEqual(data, result_data1)
        self.assertDictEqual(data, result_data2)

    # **************** end add func ***************

    # **************** delete key ***************

    def test_delete_key(self):
        """
        test delete key
        :return:
        """
        key = "APPL:SECOND:5"
        incr_key = key + ":ID"
        hash_key = key + ":HASH"
        data = {"value": 10.4}
        self.time_series.add(key, self.timestamp, data)
        self.time_series.delete(key)
        self.assertFalse(self.time_series.exists(key))
        self.assertFalse(self.time_series.exists(hash_key))
        self.assertFalse(self.time_series.exists(incr_key))

    # ****************  test remove many ********************
    def test_remove_many(self):
        data_list = self.generate_data(10)
        keys = self.prepare_many_data(data_list)

        remove_keys = keys[:5]
        rest_keys = keys[5:]

        self.time_series.remove_many(remove_keys)

        for key in remove_keys:
            self.assertFalse(self.time_series.exists(key))
            self.assertFalse(self.time_series.client.exists(key + ":ID"))
            self.assertFalse(self.time_series.client.exists(key + ":HASH"))

        for key in rest_keys:
            result = self.time_series.get_slice(key)
            self.assertListEqual(data_list, result)

    # ****************  end remove many ********************

    # **************** end delete key ***************

    # **************** test trim ********************



    # ****************  end test trim ********************

    # # ****************  test get slice  ********************


# # ****************  end get slice ********************
#
# # ****************  test add many ********************
#
# def test_add_many_exist(self):
#     key = "APPL:MINS:100"
#
#     data_list = self.generate_data(10)
#     self.time_series.add_many(key, data_list)
#     self.time_series.add_many(key, data_list)
#
#     result_data = self.time_series.get_slice(key)
#     self.assertListEqual(data_list, result_data)
#
# def test_add_many_data_twice(self):
#
#     key = "APPL:HOUR:1"
#     data_list = self.generate_data(40)
#     data_list1 = data_list[:20]
#     data_list2 = data_list[20:]
#     self.time_series.add_many(key, data_list1)
#     self.time_series.add_many(key, data_list2)
#
#     result_data = self.time_series.get_slice(key)
#     self.assertListEqual(data_list, result_data)
#
# def test_add_many_with_chunks(self):
#     key = "APPL:MINS:101"
#     data_list = self.generate_data(30)
#
#     self.time_series.add_many(key, data_list, chunks_size=5)
#
#     result_data = self.time_series.get_slice(key)
#     self.assertListEqual(data_list, result_data)
#
# # ****************  end add many ********************
#

#
# # ****************  end remove many ********************
#
# def test_flush(self):
#     key = "APPL"
#     self.time_series.add(key, self.timestamp, 1)
#     self.time_series.flush()
#     self.assertEqual(self.time_series.get_slice(key), [])
