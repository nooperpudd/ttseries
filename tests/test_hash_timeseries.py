# encoding:utf-8
import datetime
import unittest

import redis

from ttseries import RedisHashTimeSeries
from .mixin import Mixin


class RedisStoreTest(unittest.TestCase, Mixin):

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

        self.assertEqual(self.time_series.delete(key), 3)
        self.assertFalse(self.time_series.exists(key))
        self.assertFalse(self.time_series.exists(hash_key))
        self.assertFalse(self.time_series.exists(incr_key))

    def test_delete_one_timestamp(self):
        """
        test delete with only one timestamp
        :return:
        """
        key = "APPL:SECOND:6"
        data = {"value": 10.3}
        self.time_series.add(key, self.timestamp, data)
        self.time_series.delete(key, start_timestamp=self.timestamp,
                                end_timestamp=self.timestamp)
        self.assertEqual(self.time_series.count(key), 0)
        self.assertFalse(self.time_series.exist_timestamp(key, self.timestamp))

    def test_delete_with_start_timestamp(self):
        """
        test delete with start timestamp
        :return:
        """
        key = "APPL:SECOND"
        data_list = self.generate_data(10)
        self.time_series.add_many(key, data_list)

        start_timestamp = self.timestamp + 3  # delete from start timestamp

        self.time_series.delete(key, start_timestamp=start_timestamp)
        self.assertEqual(self.time_series.count(key), 3)

        for item in data_list[:]:
            if item[0] >= start_timestamp:
                data_list.remove(item)
                result = self.time_series.get(key, item[0])
                self.assertEqual(result, None)

        for time, item in data_list:
            result = self.time_series.get(key, time)
            self.assertEqual(result, item)

    def test_delete_with_end_timestamp(self):
        key = "APPL:SECOND"
        data_list = self.generate_data(10)

        self.time_series.add_many(key, data_list)

        end_timestamp = self.timestamp + 5

        self.time_series.delete(key, end_timestamp=end_timestamp)
        self.assertEqual(self.time_series.count(key), 4)

        for item in data_list[:]:
            if item[0] <= end_timestamp:
                data_list.remove(item)
                result = self.time_series.get(key, item[0])
                self.assertEqual(result, None)

        for time, item in data_list:
            result = self.time_series.get(key, time)
            self.assertEqual(result, item)

    def test_delete_with_start_and_end(self):
        key = "APPL:SECOND"
        data_list = self.generate_data(10)
        self.time_series.add_many(key, data_list)

        start_timestamp = self.timestamp + 3
        end_timestamp = self.timestamp + 6

        self.time_series.delete(key, start_timestamp=start_timestamp, end_timestamp=end_timestamp)
        self.assertEqual(self.time_series.count(key), 6)

        for item in data_list[:]:
            if start_timestamp <= item[0] <= end_timestamp:
                data_list.remove(item)
                result = self.time_series.get(key, item[0])
                self.assertEqual(result, None)

        for time, item in data_list:
            result = self.time_series.get(key, time)
            self.assertEqual(result, item)

    def test_delete_with_max_length_auto_trim(self):
        """
        :return:
        """
        key = "AAPL:DATA"
        data_list = self.generate_data(10)
        for time, item in data_list:
            self.time_series.add(key, time, item)

        new_data = {"value": 12}
        new_data2 = {"value": 13}
        new_timestamp = self.timestamp + 10
        new_timestamp2 = self.timestamp + 11

        self.time_series.add(key, new_timestamp, new_data)
        self.time_series.add(key, new_timestamp2, new_data2)

        data_list.append((new_timestamp, new_data))
        data_list.append((new_timestamp2, new_data2))

        data_list.pop(0)  # remove data list first item
        data_list.pop(0)

        start_timestamp = self.timestamp + 8

        self.time_series.delete(name=key, start_timestamp=start_timestamp)

        for item in data_list[:]:
            if item[0] >= start_timestamp:
                data_list.remove(item)
                result = self.time_series.get(key, item[0])
                self.assertEqual(result, None)
        for time, item in data_list:
            result = self.time_series.get(key, time)
            self.assertEqual(result, item)

    def test_delete_with_max_length_trim_start_and_end_timestamp(self):
        """
        :return:
        """

        key = "AAPL:DATA"
        data_list = self.generate_data(10)
        for time, item in data_list:
            self.time_series.add(key, time, item)

        new_data = {"value": 12}
        new_data2 = {"value": 13}
        new_timestamp = self.timestamp + 10
        new_timestamp2 = self.timestamp + 11

        self.time_series.add(key, new_timestamp, new_data)
        self.time_series.add(key, new_timestamp2, new_data2)

        data_list.append((new_timestamp, new_data))
        data_list.append((new_timestamp2, new_data2))

        data_list.pop(0)  # remove data list first item
        data_list.pop(0)

        start_timestamp = self.timestamp + 8
        end_timestamp = self.timestamp + 10
        self.time_series.delete(name=key,
                                start_timestamp=start_timestamp,
                                end_timestamp=end_timestamp)

        for item in data_list[:]:
            if start_timestamp <= item[0] <= end_timestamp:
                data_list.remove(item)
                result = self.time_series.get(key, item[0])
                self.assertEqual(result, None)
        for time, item in data_list:
            result = self.time_series.get(key, time)
            self.assertEqual(result, item)

    # ****************  test remove many ********************
    def test_remove_many(self):
        keys = ["APPL:DAY:" + str(key) for key in range(20)]
        data_list = self.generate_data(10)
        for key in keys:
            for timestamp, item in data_list:
                self.time_series.add(key, timestamp, item)

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

    def test_remove_many_with_start_end_timestamp(self):

        keys = ["APPL:DAY:" + str(key) for key in range(20)]
        data_list = self.generate_data(10)
        for key in keys:
            for timestamp, item in data_list:
                self.time_series.add(key, timestamp, item)

        start_timestamp = self.timestamp + 5
        end_timestamp = self.timestamp + 8

        self.time_series.remove_many(keys, start_timestamp, end_timestamp)

        for item in data_list[:]:
            if start_timestamp <= item[0] <= end_timestamp:
                data_list.remove(item)
                for key in keys:
                    result = self.time_series.get(key, item[0])
                    self.assertEqual(result, None)

    # ****************  end remove many ********************

    # **************** end delete key ***************

    # **************** test trim ********************

    def test_trim(self):
        key = "APPL:MINS:10"
        data_list = self.generate_data(20)
        self.time_series.add_many(key, data_list)

        self.time_series.trim(key, 5)

        self.assertEqual(self.time_series.count(key), 15)

        data_list = sorted(data_list, key=lambda k: k[0])

        result_data_list = data_list[5 - len(data_list):]
        result_data = self.time_series.get_slice(key)

        self.assertListEqual(result_data_list, result_data)

        trim_data_list = data_list[:5]

        for timestamp, _ in trim_data_list:
            result = self.time_series.get(key, timestamp)
            self.assertIsNone(result)

    def test_trim_length(self):
        key = "APPL:MINS:15"
        data_list = self.generate_data(10)
        self.time_series.add_many(key, data_list)

        self.time_series.trim(key, 5)
        self.assertEqual(self.time_series.count(key), 5)

    def test_trim_lgt_max_length(self):

        key = "APPL:MINS:15"
        data_list = self.generate_data(10)

        for timestamp, item in data_list:
            self.time_series.add(key, timestamp, item)
        self.time_series.trim(key, 20)

        self.assertEqual(self.time_series.count(key), 0)
        for time, item in data_list:
            self.assertEqual(self.time_series.get(key, time), None)

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
