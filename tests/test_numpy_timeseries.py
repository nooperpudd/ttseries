import datetime
import unittest

import numpy as np
import numpy.testing
import redis

from ttseries import RedisNumpyTimeSeries


class RedisNumpyTSTestMixin(object):
    """
    """
    def test_get_slice(self):
        key = "AAPL:SECOND"
        array = self.prepare_numpy_data(10)
        self.time_series.add_many(key, array)

        results = self.time_series.get_slice(key)

        numpy.testing.assert_array_equal(results, array)

    def test_iter(self):
        key = "AAPL:SECOND"
        data_array = self.prepare_numpy_data(10)
        self.time_series.add_many(key, data_array)

        for array in self.time_series.iter(key):
            self.assertTrue(array in data_array)


class RedisNumpyTSTest(unittest.TestCase, RedisNumpyTSTestMixin):
    def setUp(self):
        redis_client = redis.StrictRedis()
        self.time_series = RedisNumpyTimeSeries(redis_client=redis_client,
                                                max_length=10)
        self.timestamp = datetime.datetime.now().timestamp()

    def tearDown(self):
        self.time_series.flush()

    def prepare_numpy_data(self, length):
        data_list = []
        for i in range(length):
            timestamp = self.timestamp + i
            data_list.append((timestamp, i))
        array = np.array(data_list)
        return array

    def test_get(self):
        key = "AAPL:SECOND"
        array = self.prepare_numpy_data(10)

        self.time_series.add_many(key, array)
        timestamp = array[0][0]
        result = self.time_series.get(key, timestamp)

        numpy.testing.assert_array_equal(result, array[0])


class RedisNumpyDtypeTSTest(unittest.TestCase, RedisNumpyTSTestMixin):

    def setUp(self):
        redis_client = redis.StrictRedis()
        self.dtype = [("timestamp", "float64"), ("value", "int")]
        self.time_series = RedisNumpyTimeSeries(redis_client=redis_client,
                                                max_length=10, dtype=self.dtype,
                                                timestamp_column_name="timestamp")
        self.timestamp = datetime.datetime.now().timestamp()

    def tearDown(self):
        self.time_series.flush()

    def prepare_numpy_data(self, length):
        data_list = []

        for i in range(length):
            timestamp = self.timestamp + i
            data_list.append((timestamp, i))
        array = np.array(data_list, dtype=self.dtype)
        return array

    def test_get(self):
        key = "AAPL:SECOND"
        array = self.prepare_numpy_data(10)

        self.time_series.add_many(key, array)
        result = self.time_series.get(key, array["timestamp"][0])

        numpy.testing.assert_array_equal(result, array[0])
