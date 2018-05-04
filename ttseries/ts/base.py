# encoding:utf-8
import contextlib
import functools
import threading

import redis

from ttseries import serializers
from ttseries.exceptions import SerializerError


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


class RedisClient(object):
    """
    """
    incr_format = "{key}:ID"  # as the auto increase id

    def __init__(self, redis_client, max_length=100000, transaction=True,
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

    def count(self, name: str):
        """
        :param name:
        :return: int
        """
        incr_key = self.incr_format.format(key=name)

        length = self.client.get(incr_key)
        if length >= self.max_length:
            return self.max_length
        else:
            return int(length)

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
