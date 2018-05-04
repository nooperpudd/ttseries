# encoding:utf-8
import datetime
import unittest
import redis
import pytest


from ttseries import RedisHashTimeSeries


class RedisStoreTest(unittest.TestCase):
    def setUp(self):
        redis_client = redis.StrictRedis()
        self.time_series = RedisHashTimeSeries(redis_client,max_length=10)
        self.timestamp = datetime.datetime.now().timestamp()

    def tearDown(self):
        self.time_series.flush()

    def generate_data(self, length):
        data_list = []
        for i in range(length):
            timestamp = self.timestamp + i
            data = {"value": i}
            data_list.append((timestamp, data))
        return data_list

    # **************** add func ***************

    def test_add(self):
        key = "APPL:SECOND:1"

        data = {
            "value": 21,
            "volume": 11344.34,
            "asks": [{"ask_1": 10, "price": 21}],
            "bid": [{"bid_1": 10, "price": 20}]
        }
        result = self.time_series.add(key, self.timestamp, data)
        self.assertTrue(result)

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

    def test_add_repeated_timestamp(self):
        """
        ensure each timestamp mapping to each value
        """
        key = "APPL:SECOND:3"
        data = {"value": 21}
        data2 = {"value": 33}
        result = self.time_series.add(key, self.timestamp, data)
        result2 = self.time_series.add(key, self.timestamp, data2)
        self.assertTrue(result)
        self.assertFalse(result2)

    # **************** end add func ***************

    def test_add_max_length(self):
        key = "APPL:SECOND"
        results = self.generate_data(10)
    def test_add_

    def test_add_exception_fails(self):
        pass
    #
    def test_get(self):

        key = "APPL:SECOND:4"
        data = {"value": 10.3}
        self.time_series.add(key, self.timestamp, data)
        result = self.time_series.get(key, self.timestamp)
        self.assertDictEqual(data, result)
    #
    # # **************** delete key ***************
    #
    # def test_delete_key(self):
    #     """
    #     test delete key
    #     :return:
    #     """
    #     key = "APPL:SECOND:5"
    #     incr_key = key + ":ID"
    #     hash_key = key + ":HASH"
    #     data = {"value": 10.4}
    #     self.time_series.add(key, self.timestamp, data)
    #
    #     self.assertEqual(self.time_series.delete(key), 3)
    #     self.assertFalse(self.time_series.exists(key))
    #     self.assertFalse(self.time_series.exists(hash_key))
    #     self.assertFalse(self.time_series.exists(incr_key))
    #
    # def test_delete_one_timestamp(self):
    #     """
    #     test delete with only one timestamp
    #     :return:
    #     """
    #     key = "APPL:SECOND:6"
    #     data = {"value": 10.3}
    #     self.time_series.add(key, self.timestamp, data)
    #     self.time_series.delete(key, start_timestamp=self.timestamp,
    #                             end_timestamp=self.timestamp)
    #     self.assertEqual(self.time_series.count(key), 0)
    #     self.assertFalse(self.time_series.exists(key, self.timestamp))
    #
    # def test_delete_with_start_timestamp(self):
    #     """
    #     test delete with start timestamp
    #     :return:
    #     """
    #     key = "APPL:SECOND"
    #     data_list = self.generate_data(10)
    #     self.time_series.add_many(key, data_list)
    #
    #     start_timestamp = self.timestamp + 3
    #
    #     self.time_series.delete(key, start_timestamp=start_timestamp)
    #     self.assertEqual(self.time_series.count(key), 3)
    #
    #     for item in data_list[:]:
    #         if item[0] >= start_timestamp:
    #             data_list.remove(item)
    #
    #     result_data = self.time_series.get_slice(key, start=self.timestamp, end=self.timestamp + 3)
    #
    #     self.assertListEqual(result_data, data_list)
    #
    # def test_delete_with_end_timestamp(self):
    #     key = "APPL:SECOND"
    #     data_list = self.generate_data(10)
    #
    #     self.time_series.add_many(key, data_list)
    #
    #     end_timestamp = self.timestamp + 5
    #
    #     self.time_series.delete(key, end_timestamp=end_timestamp)
    #     self.assertEqual(self.time_series.count(key), 4)
    #
    #     for item in data_list[:]:
    #         if item[0] <= end_timestamp:
    #             data_list.remove(item)
    #
    #     result_data = self.time_series.get_slice(key, start=end_timestamp)
    #
    #     self.assertListEqual(result_data, data_list)
    #
    # def test_delete_with_start_and_end(self):
    #     key = "APPL:SECOND"
    #     data_list = self.generate_data(10)
    #     self.time_series.add_many(key, data_list)
    #
    #     start_timestamp = self.timestamp + 3
    #     end_timestamp = self.timestamp + 6
    #
    #     self.time_series.delete(key, start_timestamp=start_timestamp, end_timestamp=end_timestamp)
    #     self.assertEqual(self.time_series.count(key), 6)
    #
    #     for item in data_list[:]:
    #         if start_timestamp <= item[0] <= end_timestamp:
    #             data_list.remove(item)
    #
    #     result_data = self.time_series.get_slice(key)
    #
    #     self.assertListEqual(result_data, data_list)
    #
    # # **************** end delete key ***************
    #
    # # **************** test trim ********************
    #
    # def test_trim(self):
    #     key = "APPL:MINS:10"
    #     data_list = self.generate_data(20)
    #     self.time_series.add_many(key, data_list)
    #
    #     self.time_series.trim(key, 5)
    #
    #     self.assertEqual(self.time_series.count(key), 15)
    #
    #     data_list = sorted(data_list, key=lambda k: k[0])
    #
    #     result_data_list = data_list[5 - len(data_list):]
    #     result_data = self.time_series.get_slice(key)
    #
    #     self.assertListEqual(result_data_list, result_data)
    #
    #     trim_data_list = data_list[:5]
    #
    #     for timestamp, _ in trim_data_list:
    #         result = self.time_series.get(key, timestamp)
    #         self.assertIsNone(result)
    #
    # def test_trim_length_none(self):
    #     key = "APPL:MINS:15"
    #     data_list = self.generate_data(30)
    #     self.time_series.add_many(key, data_list)
    #
    #     self.time_series.trim(key)
    #     self.assertEqual(self.time_series.count(key), 0)
    #
    #     result_data_list = data_list[1000:]
    #     result_data = self.time_series.get_slice(key)
    #     self.assertListEqual(result_data_list, result_data)
    #
    # # ****************  end test trim ********************
    #
    # # ****************  test get slice  ********************
    # def test_get_slice_with_key(self):
    #     key = "APPL:MINS:1"
    #     data_list = self.generate_data(30)
    #     self.time_series.add_many(key, data_list)
    #
    #     result_data = self.time_series.get_slice(key)
    #     self.assertListEqual(data_list, result_data)
    #
    # def test_get_slice_with_start_timestamp(self):
    #     """
    #     test get slice with start timestamp
    #     :return:
    #     """
    #     key = "APPL:MINS:2"
    #     data_list = self.generate_data(30)
    #     self.time_series.add_many(key, data_list)
    #
    #     start = self.timestamp + 6
    #     result_data = self.time_series.get_slice(key, start=start)
    #
    #     filter_data = list(filter(lambda seq: seq[0] >= start, data_list))
    #
    #     self.assertListEqual(result_data, filter_data)
    #
    # def test_get_slice_with_end_timestamp(self):
    #     """
    #     test get slice only with end timestamp
    #     :return:
    #     """
    #     key = "APPL:MINS:6"
    #     data_list = self.generate_data(30)
    #     self.time_series.add_many(key, data_list)
    #     end = self.timestamp + 10
    #     result_data = self.time_series.get_slice(key, end=end)
    #     filter_data = list(filter(lambda seq: seq[0] <= end, data_list))
    #     self.assertEqual(result_data, filter_data)
    #
    # def test_get_slice_with_timestamp(self):
    #     key = "APPL:MINS:4"
    #     data_list = self.generate_data(30)
    #     self.time_series.add_many(key, data_list)
    #
    #     start = self.timestamp + 6
    #     end = self.timestamp + 10
    #     result_data = self.time_series.get_slice(key, start, end)
    #     filter_data = list(filter(lambda seq: start <= seq[0] <= end, data_list))
    #     self.assertEqual(result_data, filter_data)
    #
    # def test_get_slice_with_start_length(self):
    #     key = "APPL:MINS:5"
    #     data_list = self.generate_data(30)
    #     self.time_series.add_many(key, data_list)
    #     result_data = self.time_series.get_slice(key, start_index=0, limit=10)
    #     filter_data = data_list[:10]
    #     self.assertEqual(result_data, filter_data)
    #
    # def test_get_slice_with_order(self):
    #     key = "APPL:MINS:8"
    #     data_list = self.generate_data(30)
    #     self.time_series.add_many(key, data_list)
    #     result_data = self.time_series.get_slice(key, asc=False)
    #     filter_data = list(reversed(data_list))
    #     self.assertListEqual(result_data, filter_data)
    #
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
    # # ****************  test remove many ********************
    # def test_remove_many(self):
    #     keys = ["APPL:DAY:" + str(key) for key in range(20)]
    #     data_list = self.generate_data(50)
    #     for key in keys:
    #         self.time_series.add_many(key, data_list)
    #
    #     remove_keys = keys[:5]
    #     rest_keys = keys[5:]
    #
    #     self.time_series.remove_many(remove_keys)
    #
    #     for key in remove_keys:
    #         self.assertFalse(self.time_series.exists(key))
    #         self.assertFalse(self.time_series.client.exists(key + ":ID"))
    #         self.assertFalse(self.time_series.client.exists(key + ":HASH"))
    #
    #     for key in rest_keys:
    #         result = self.time_series.get_slice(key)
    #         self.assertListEqual(data_list, result)
    #
    # # ****************  end remove many ********************
    #
    # # ****************  end remove many ********************
    #
    # def test_flush(self):
    #     key = "APPL"
    #     self.time_series.add(key, self.timestamp, 1)
    #     self.time_series.flush()
    #     self.assertEqual(self.time_series.get_slice(key), [])
