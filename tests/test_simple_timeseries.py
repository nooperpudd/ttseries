# encoding:utf-8
import datetime
import unittest

import redis

from ttseries import RedisHashTimeSeries
from . import TestMixin


class SimpleTsTest(unittest.TestCase, TestMixin):

    def setUp(self):
        redis_client = redis.StrictRedis()
        self.time_series = RedisHashTimeSeries(redis_client, max_length=10)
        self.timestamp = datetime.datetime.now().timestamp()

    def tearDown(self):
        self.time_series.flush()

    def test_add_max_length(self):
        """
        :return:
        """
        pass

    def test_get(self):
        """
        :return:
        """
        pass

    def test_count(self):
        key = "APPL:SECOND"

        results = self.generate_data(10)
        for item in results:
            self.time_series.add(key, item[0], item[1])

        self.assertEqual(self.time_series.count(key), 10)

    def test_count_with_timestamp(self):
        pass

    def test_length(self):
        pass

    def test_add_many(self):
        pass

    def test_add_many_max_length(self):
        pass

    def test_delete(self):
        pass

    def test_delete_with_timestamp(self):
        pass

    def test_trim(self):
        pass

    def test_get_slice(self):
        pass

    def test_get_slice_with_desc(self):
        pass

    def test_get_slice_with_start_end(self):
        pass

    def test_get_slice_with_start_limit(self):
        pass

    def test_get_slice_with_start_end_desc(self):
        pass
