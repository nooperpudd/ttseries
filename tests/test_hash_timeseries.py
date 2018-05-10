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
    def test_get_slice_with_key(self):
        key = "APPL:MINS:1"
        data_list = self.generate_data(10)

        for timestamp, item in data_list:
            self.time_series.add(key, timestamp, item)

        result_data = self.time_series.get_slice(key)
        self.assertListEqual(data_list, result_data)

    def test_get_slice_with_key_desc(self):
        key = "APPL:MINS:1"
        data_list = self.generate_data(10)

        for timestamp, item in data_list:
            self.time_series.add(key, timestamp, item)
        result_data = self.time_series.get_slice(key, asc=False)

        reversed_data = sorted(data_list, key=lambda tup: tup[0], reverse=True)

        self.assertListEqual(reversed_data, result_data)

    def test_get_slice_with_start_timestamp(self):
        """
        test get slice with start timestamp
        :return:
        """
        key = "APPL:MINS:2"
        data_list = self.generate_data(10)
        for timestamp, item in data_list:
            self.time_series.add(key, timestamp, item)
        start = self.timestamp + 6
        result_data = self.time_series.get_slice(key, start_timestamp=start)

        filter_data = list(filter(lambda seq: seq[0] >= start, data_list))

        self.assertEqual(result_data, filter_data)

    def test_get_slice_with_gt_start_timestamp(self):

        key = "APPL:MINS:2"

        data_list = self.generate_data(10)
        for timestamp, item in data_list:
            self.time_series.add(key, timestamp, item)

        start = self.timestamp + 6
        start_timestamp = "(" + str(start)
        result_data = self.time_series.get_slice(key,
                                                 start_timestamp=start_timestamp)

        filter_data = list(filter(lambda seq: seq[0] > start, data_list))

        self.assertEqual(result_data, filter_data)

    def test_get_slice_with_start_timestamp_limit(self):

        key = "APPL:MINS:2"

        data_list = self.generate_data(10)
        for timestamp, item in data_list:
            self.time_series.add(key, timestamp, item)

        start = self.timestamp + 5
        result_data = self.time_series.get_slice(key,
                                                 start_timestamp=start, limit=3)

        filter_data = list(filter(lambda seq: seq[0] >= start, data_list))
        filter_data = filter_data[:3]

        self.assertEqual(result_data, filter_data)

    def test_get_slice_with_start_timestamp_desc(self):

        key = "APPL:MINS:2"

        data_list = self.generate_data(10)
        for timestamp, item in data_list:
            self.time_series.add(key, timestamp, item)

        start = self.timestamp + 6
        start_timestamp = "(" + str(start)
        result_data = self.time_series.get_slice(key,
                                                 start_timestamp=start_timestamp)

        filter_data = list(filter(lambda seq: seq[0] > start, data_list))

        self.assertEqual(result_data, filter_data)

    def test_get_slice_with_end_timestamp(self):
        """
        test get slice only with end timestamp
        :return:
        """
        key = "APPL:MINS:6"
        data_list = self.generate_data(10)
        for timestamp, item in data_list:
            self.time_series.add(key, timestamp, item)
        end = self.timestamp + 6

        result_data = self.time_series.get_slice(key, end_timestamp=end)
        filter_data = list(filter(lambda seq: seq[0] <= end, data_list))
        self.assertEqual(result_data, filter_data)

    def test_get_slice_with_timestamp(self):

        key = "APPL:MINS:4"
        data_list = self.generate_data(10)
        for timestamp, item in data_list:
            self.time_series.add(key, timestamp, item)

        start = self.timestamp + 6
        end = self.timestamp + 10
        result_data = self.time_series.get_slice(key, start, end)
        filter_data = list(filter(lambda seq: start <= seq[0] <= end, data_list))
        self.assertEqual(result_data, filter_data)

    def test_get_slice_with_start_length(self):
        key = "APPL:MINS:5"
        data_list = self.generate_data(10)
        for timestamp, item in data_list:
            self.time_series.add(key, timestamp, item)

        result_data = self.time_series.get_slice(key, start_index=0, limit=5)
        filter_data = data_list[:5]
        self.assertEqual(result_data, filter_data)

    def test_get_slice_with_order(self):

        key = "APPL:MINS:8"
        data_list = self.generate_data(10)
        for timestamp, item in data_list:
            self.time_series.add(key, timestamp, item)

        result_data = self.time_series.get_slice(key, asc=False)
        filter_data = list(reversed(data_list))
        self.assertListEqual(result_data, filter_data)

    def test_get_slice_start_end_time_order(self):

        key = "APPL:MINS:8"
        data_list = self.generate_data(10)
        for timestamp, item in data_list:
            self.time_series.add(key, timestamp, item)

        start_timestamp = self.timestamp + 3
        end_timestamp = self.timestamp + 6

        result_data = self.time_series.get_slice(key, start_timestamp=start_timestamp,
                                                 end_timestamp=end_timestamp,
                                                 asc=False)

        data_list = data_list[3:7]
        filter_data = list(reversed(data_list))
        self.assertListEqual(result_data, filter_data)

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
