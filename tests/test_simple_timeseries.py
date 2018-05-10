# encoding:utf-8
import datetime
import unittest

import redis

from ttseries import RedisSampleTimeSeries
from .mixin import Mixin


class SimpleTsTest(unittest.TestCase, Mixin):

    def setUp(self):
        redis_client = redis.StrictRedis()
        self.time_series = RedisSampleTimeSeries(redis_client, max_length=10)
        self.timestamp = datetime.datetime.now().timestamp()

    def tearDown(self):
        self.time_series.flush()

    def test_add_many(self):
        pass

    def test_add_many_max_length(self):
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
