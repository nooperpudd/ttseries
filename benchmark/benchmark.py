# encoding:utf-8

import datetime

import numpy
import pytest
import redis

import ttseries
from ttseries.serializers import DumpySerializer


# test time series RedisSimpleTimeSeries

class InitData(object):
    def __init__(self, ):

        now = datetime.datetime.now()
        self.timestamp = now.timestamp()

    def prepare_data(self, length=1000):
        results = []
        for i in range(length):
            results.append((self.timestamp + i, i))
        return results

    def prepare_data_with_dict(self, length=1000):

        results = []
        for i in range(length):
            results.append((self.timestamp + i, {"value": i}))
        return results

    def prepare_data_with_list(self, length):
        results = []
        for i in range(length):
            results.append((self.timestamp + i, [i + 1, "A"]))
        return results

    def prepare_array(self, length):
        results = []
        for i in range(length):
            results.append((self.timestamp + i, i))
        array = numpy.array(results)
        return array


init_data = InitData()
key = "APPL:SECOND:10"


@pytest.fixture()
def simple_timeseries_dumpy():
    redis_client = redis.StrictRedis()
    series = ttseries.RedisSampleTimeSeries(redis_client, serializer_cls=DumpySerializer)
    yield series
    series.flush()


@pytest.fixture()
def simple_time_series():
    redis_client = redis.StrictRedis()
    series = ttseries.RedisSampleTimeSeries(redis_client)
    yield series
    series.flush()


@pytest.mark.usefixtures("simple_timeseries_dumpy")
@pytest.mark.benchmark(group="simple", disable_gc=True)
@pytest.mark.parametrize('data', [init_data.prepare_data(1000),
                                  init_data.prepare_data(10000),
                                  init_data.prepare_data(100000)])
def test_add_simple_timeseries_without_serializer(simple_timeseries_dumpy,
                                                  benchmark,
                                                  data):
    @benchmark
    def bench():
        simple_timeseries_dumpy.add_many(name=key, timestamp_pairs=data)
        simple_timeseries_dumpy.flush()


@pytest.mark.usefixtures("simple_timeseries_dumpy")
@pytest.mark.benchmark(group="simple", disable_gc=True)
@pytest.mark.parametrize("length", [1000, 10000, 100000])
def test_get_simple_timeseries_dumpy_serializer(simple_timeseries_dumpy,
                                                benchmark,
                                                length):
    simple_timeseries_dumpy.add_many(key,
                                     init_data.prepare_data(length))

    @benchmark
    def bench():
        simple_timeseries_dumpy.get_slice(key, init_data.timestamp,
                                          limit=length)


@pytest.mark.usefixtures("simple_time_series")
@pytest.mark.benchmark(group="simple", disable_gc=True)
@pytest.mark.parametrize('data', [init_data.prepare_data_with_dict(1000),
                                  init_data.prepare_data_with_dict(10000),
                                  init_data.prepare_data_with_dict(100000)])
def test_simple_timeseries_serializer(simple_time_series, benchmark, data):
    @benchmark
    def bench():
        simple_time_series.add_many(name=key, timestamp_pairs=data)
        simple_time_series.flush()


@pytest.mark.usefixtures("simple_time_series")
@pytest.mark.benchmark(group="simple", disable_gc=True)
@pytest.mark.parametrize("length", [1000, 10000, 100000])
def test_get_simple_timeseries_serializer(simple_time_series,
                                          benchmark,
                                          length):
    simple_time_series.add_many(key,
                                init_data.prepare_data_with_dict(length))

    @benchmark
    def bench():
        simple_time_series.get_slice(key, init_data.timestamp,
                                     limit=length)

# @benchmark
# def test_simple_timeseries_serializer():
#     pass
#
# @benchmark
# def test_simple_timeseries_with_compress():
#     pass
#
# @benchmark
# def test_simple_timeseries_with_numpy():
#     pass
#
# @benchmark
# def test_hash_timeseries_without_serializer():
#     pass
#
# @benchmark
# def test_hash_timeseries_serializer():
#     pass
#
# @benchmark
# def test_hash_timeseries_with_compress():
#     pass
#
# @benchmark
# def test_hash_timeseries_with_numpy():
#     pass
#
