# encoding:utf-8
import contextlib
import functools
import itertools
import threading
from operator import itemgetter

import numpy as np
import redis

from ttseries import serializers
from ttseries.exceptions import SerializerError, RedisTimeSeriesError


class RedisTSBase(object):
    """
    Redis Time-series base class

    in redis sorted sets, if want to filter the timestamp
    with ">=", ">" or "<=", "<".

    if want to filter the start timestamp >10,
    the min or start timestamp could be `(10`,

    or want to filter the start timestamp>=10
    the min or start timestamp could be `10`

    for end timestamp<10:
    the max or end timestamp could be `(10`

    for end timestamp<=10:
    the max or end timestamp could be `10`

    """

    # todo support redis cluster
    # todo support parllizem and multi threading
    # todo implement auto moving windows

    def __init__(self, redis_client, max_length=100000,
                 transaction=True,
                 use_numpy = False,
                 serializer_cls=serializers.MsgPackSerializer,
                 compressor_cls=None):
        """
        :param redis_client: redis client instance, only test with redis-py client.
        :param max_length: int, max length of data to store the time-series data.
        :param transaction: bool, to ensure all the add or delete commands can be executed atomically
        :param use_numpy: bool, support numpy array to store and get data.
        :param serializer_cls: serializer class, serializer the data
        :param compressor_cls: compress class, compress the data
        """
        self._redis_client = redis_client
        self.max_length = max_length
        self.transaction = transaction
        self.use_numpy = use_numpy
        self._lock = threading.RLock()

        if issubclass(serializer_cls, serializers.BaseSerializer):
            self._serializer = serializer_cls()
        else:
            raise SerializerError("Serializer class must inherit from "
                                  "ttseries.serializers.BaseSerializer abstract class")

        self._compress = compressor_cls  # todo implement

    @property
    @functools.lru_cache(maxsize=4096)
    def client(self):
        """
        :return: redis client
        """
        return self._redis_client

    @contextlib.contextmanager
    def _pipe_acquire(self):
        """
        redis pipeline
        :return:
        """
        yield self.client.pipeline(transaction=self.transaction)

    def flush(self):
        """
        flush database
        :return:
        """
        self.client.flushdb()

    def length(self, name):
        """
        Time complexity: O(1)
        get the time-series length from a key
        :param name: redis key
        :return: int
        """
        return self.client.zcard(name)

    def count(self, name, start_timestamp: float = None, end_timestamp: float = None):
        """
        Time complexity: O(log(N)) with N being
        the number of elements in the sorted sets.
        :param name: redis key
        :param start_timestamp: float, start timestamp
        :param end_timestamp: float, end timestamp
        :return: int
        """
        if start_timestamp is None:
            start_timestamp = "-inf"
        if end_timestamp is None:
            end_timestamp = "+inf"
        return self.client.zcount(name, min=start_timestamp, max=end_timestamp)

    def exists(self, name):
        """
        exist key in name
        :param name: redis key
        :return: bool
        """
        return self.client.exists(name)

    def exist_timestamp(self, name, timestamp) -> bool:
        """
        Time complexity: O(log(N))
        check a timestamp exist in redis sorted sets
        :param name:
        :param timestamp:
        :return:
        """
        return bool(self.client.zcount(name, min=timestamp, max=timestamp))

    def transaction_pipe(self, pipe_func, watch_keys=None, *args, **kwargs):
        """
        Convenience callable `func` as executable in a
        transaction while watching all keys specified in `watches`.
        The 'func' callable should expect a Pipeline object as its first argument.
        :param pipe_func: function
        :param watch_keys: redis watch keys
        :param args:
        :param kwargs:
        :return:
        """
        with self._lock, self._pipe_acquire() as pipe:
            while True:
                try:
                    if watch_keys:
                        pipe.watch(watch_keys)
                    pipe.multi()

                    if callable(pipe_func):
                        pipe_func(pipe, *args, **kwargs)

                    return pipe.execute()

                except redis.exceptions.WatchError:
                    continue
                finally:
                    pipe.reset()

    def get_slice_mixin(self):
        """
        :return:
        """
        pass

    def validate_key(self, name):
        """
        validate redis key can't contains specific names
        :param name:
        """
        if ":HASH" in name or ":ID" in name:
            raise RedisTimeSeriesError("Key can't contains `:HASH`, `:ID` values.")

    def repeated_array(self, array_data):
        """
        :param array_data: [(timestamp,data),...]
        :return:
        """
        timestamps, _ = itertools.zip_longest(*array_data)
        timestamps_set = set()

        for timestamp in timestamps:
            if timestamp in timestamps_set:
                raise RedisTimeSeriesError("repeated timestamps:", timestamp)
            else:
                timestamps_set.add(timestamp)


    def _add_many_validate(self, name, array_data,
                           timestamp_column_name=None,
                           timestamp_column_index=0):
        """
        before to insert the data into redis,
        auto to trim the data exists in redis,
        and validate the timestamp already exist in redis
        :param name: redis key
        :param array_data: array data
        :param timestamp_column_name: str, timestamp numpy column name
        :param timestamp_column_index: int, timestamp numpy column index
        :return: sorted array data
        """
        array_length = len(array_data)

        if isinstance(array_data, list):

            self.repeated_array(array_data)

            array_data = sorted(array_data, key=itemgetter(0))
            end_timestamp = array_data[-1][0]  # max
            start_timestamp = array_data[0][0]  # min

        # numpy array
        elif isinstance(array_data, np.ndarray):

            if timestamp_column_name:

                timestamp_array = array_data[timestamp_column_name].astype("float64")

                if len(np.unique(timestamp_array)) != len(array_data):
                    raise RedisTimeSeriesError("repeated timestamps in array data")

                array_data[timestamp_column_name] = timestamp_array

                array_data = np.sort(array_data, order=[timestamp_column_name])
                start_timestamp = timestamp_array.min()
                end_timestamp = timestamp_array.max()

            else:

                timestamp_array = array_data[:, timestamp_column_index].astype("float64")

                if len(np.unique(timestamp_array)) != len(timestamp_array):
                    raise RedisTimeSeriesError("repeated timestamps in array data")

                array_data[:, timestamp_column_index] = timestamp_array

                array_data = np.sort(array_data, axis=timestamp_column_index)
                start_timestamp = timestamp_array.min()
                end_timestamp = timestamp_array.max()
        else:
            raise RedisTimeSeriesError("nonsupport array data type")

        # auto trim array

        if array_length + self.length(name) >= self.max_length:
            trim_length = array_length + self.length(name) - self.max_length
            self.trim(name, trim_length)

        if array_length > self.max_length:
            array_data = array_data[array_length - self.max_length:]

        # validate timestamp exists
        exist_length = self.count(name, start_timestamp, end_timestamp)

        if exist_length > 0:

            for array in self.get_slice(name, start_timestamp, end_timestamp):

                if self.use_numpy:
                    pass
                else:

                    filter_timestamps, _ = itertools.zip_longest(*array)
                    timestamps, _ = itertools.zip_longest(*array_data)
                    common_timestamps = set(filter_timestamps) & set(timestamps)
                    if common_timestamps:
                        raise RedisTimeSeriesError("add duplicated timestamp into redis", common_timestamps)
        else:
            return array_data
