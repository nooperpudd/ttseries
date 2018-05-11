# encoding:utf-8
import contextlib
import functools
import threading
from operator import itemgetter

import numpy as np
import redis

from ttseries import serializers
from ttseries.exceptions import SerializerError, RedisTimeSeriesException


# def _transaction(watch_arg=None, use_pipe=True, ):
#     """
#     wrapper class
#     :return:
#     """
#
#     def wrapper(func):
#         """
#         :return:
#         """
#
#         @functools.wraps(func)
#         def inner(self, *args, **kwargs):
#             """
#             :param self: class instance
#             :param args:
#             :param kwargs:
#             :return:
#             """
#             trans = getattr(self, "transaction")
#             redis_client = getattr(self, "client", None)
#
#             call_args = inspect.getcallargs(func, **kwargs)
#             watch_value = call_args.get(watch_arg)
#             if redis_client and trans:
#
#                 result = None
#                 try:
#                     redis_client.watch(watch_value)
#                     result = func(self, *args, **kwargs)
#                 except redis.WatchError:
#                     continue
#                 else:
#                     redis_client.unwatch()
#                     return result
#
#
#             else:
#                 return func(self, *args, **kwargs)
#
#         return inner
#
#     return wrapper


class RedisTSBase(object):
    """
    """
    incr_format = "{key}:ID"  # as the auto increase id

    def __init__(self, redis_client: redis.StrictRedis, max_length=100000, transaction=True,
                 serializer_cls=serializers.MsgPackSerializer,
                 compressor_cls=None):
        """
        :param redis_client:
        :param max_length: store redis data by key with max length.
        :param transaction:
        :param serializer_cls:
        :param compressor_cls:
        """
        self._redis_client = redis_client
        self.max_length = max_length
        self.transaction = transaction
        self._lock = threading.RLock()

        if issubclass(serializer_cls, serializers.BaseSerializer):
            self._serializer = serializer_cls()
        else:
            raise SerializerError("Serializer class must base in BaseSerializer abstract class")

        self._compress = compressor_cls

    @property
    @functools.lru_cache(maxsize=4096)
    def client(self):
        """
        :return:
        """
        return self._redis_client

    @contextlib.contextmanager
    def _pipe_acquire(self):
        """
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
        :return:
        """
        return self.client.zcard(name)

    def count(self, name, start_timestamp=None, end_timestamp=None):
        """
        Time complexity: O(log(N)) with N being
        the number of elements in the sorted set.
        :param name:
        :param start_timestamp:
        :param end_timestamp:
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
        :param name:
        :return:
        """
        return self.client.exists(name)

    def exist_timestamp(self, name, timestamp) -> bool:
        """
        :param name:
        :param timestamp:
        :return:
        """
        # Time complexity: O(log(N))
        return bool(self.client.zcount(name, min=timestamp, max=timestamp))

    def transaction_pipe(self, pipe_func, watch_keys=None, *args, **kwargs):
        """
        https://github.com/andymccurdy/redis-py/pull/560/files
        :param watch_keys:
        :param pipe_func:
        :param args:
        :param kwargs:
        :return:
        """
        with self._pipe_acquire() as pipe:
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

    def _add_many_validate(self, name, array_data):
        """
        :return:
        """
        array_length = len(array_data)

        if array_length + self.length(name) >= self.max_length:
            trim_length = array_length + self.length(name) - self.max_length
            self.trim(name, trim_length)
        if array_length > self.max_length:

            array_data = array_data[array_length - self.max_length:]

        if isinstance(array_data, list):
            # todo maybe other way to optimize this filter code
            sorted_data = sorted(array_data, key=itemgetter(0))
            end_timestamp = sorted_data[-1][0]  # max
            start_timestamp = sorted_data[0][0]  # min

        elif isinstance(array_data, np.ndarray):
            start_timestamp = array_data["timestamp"].min()
            end_timestamp = array_data["timestamp"].max()
        else:
            raise RedisTimeSeriesException("nonsupport array data type")

        if self.count(name, start_timestamp, end_timestamp) > 0:
            raise RedisTimeSeriesException("exist timestamp in redis")
        else:
            return array_data

