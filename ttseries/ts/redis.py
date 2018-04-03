# encoding:utf-8

import contextlib
import functools
import itertools
from operator import itemgetter

from ttseries import serializers
from ttseries.exceptions import (SerializerError)


def transaction():
    """
    wrapper class
    :return:
    """

    def wrapper(func):
        """
        :return:
        """

        @functools.wraps(func)
        def inner(*args, **kwargs):
            """
            :param args:
            :param kwargs:
            :return:
            """

        return inner

    return wrapper


class RedisTimeSeries(object):
    """
    Redis to save time-series data
    use redis sorted set as the time-series
    sorted as the desc
    support max length 2**63-1
    """
    # todo support redis cluster
    # todo support lte or gte
    # todo support redis transaction

    # todo support parllizem and mulit threading
    # todo support lock, when add large amount data
    # todo implement redis lock
    # todo support numpy, best for memory
    # todo support max time-series length
    # todo test many item data execute how much could support 10000? 100000? 10000000?
    # todo max length to auto trim the redis data
    # todo implement auto move windows moving

    # serializer_class = serializers.MsgPackSerializer
    # compress_class = compress.

    incr_format = "{key}:ID"  # as the auto increase id
    hash_format = "{key}:HASH"  # as the hash set id

    def __init__(self, redis_client, max_length=100000,
                 serializer_cls=serializers.MsgPackSerializer,
                 compressor_cls=None):
        """
        :param redis_client:
        :param max_length: store redis data by key with max length.
        :param serializer_cls:
        :param compressor_cls:
        """
        self._redis_client = redis_client
        self.max_length = max_length
        if issubclass(serializer_cls, serializers.BaseSerializer):
            self._serializer = serializer_cls()
        else:
            raise SerializerError("")
        if compressor_cls:
            self._compress = None

    @property
    @functools.lru_cache()
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
        yield self.client.pipeline()

    def count(self, name: str):
        """
        :param name:
        :return: int
        """
        incr_key = self.incr_format.format(key=name)
        return int(self.client.get(incr_key))

    def add(self, name: str, timestamp: float, data):
        """
        incr -> result
        hmset key field value
        zadd (sorted set) key score(timestamp) value

        ensure only one timestamp corresponding one value
        :param name: key name
        :param timestamp: timestamp: float
        :param data:
        :return: bool
        """
        dumps_data = self._serializer.dumps(data)
        incr_key = self.incr_format.format(key=name)
        hash_key = self.hash_format.format(key=name)

        if not self.exists(name, timestamp):
            key_id = self.client.incr(incr_key)
            dumps_dict = {key_id: dumps_data}
            with self._pipe_acquire() as pipe:
                pipe.multi()
                pipe.zadd(name, timestamp, key_id)
                pipe.hmset(hash_key, dumps_dict)
                results = pipe.execute()
                return True if all(results) else False
        else:
            return False

    def exists(self, name, timestamp=None) -> bool:
        """
        :param name:
        :param timestamp:
        :return: bool
        """
        if timestamp:
            # Time complexity: O(log(N))
            result = self.client.zcount(name, min=timestamp, max=timestamp)
            return bool(result)
        else:
            return self.client.exists(name)

    def get(self, name, timestamp):
        """
        :param name:
        :param timestamp:
        :return:
        """
        hash_key = self.hash_format.format(key=name)

        result_id = self.client.zrangebyscore(name, min=timestamp, max=timestamp)
        if result_id:
            data = self.client.hmget(hash_key, result_id)
            # only one item
            return self.serializer.loads(data[0])

    def delete(self, name, start_timestamp=None, end_timestamp=None):
        """
        delete one key item or delete by timestamp order
        :param name:
        :param start_timestamp:
        :param end_timestamp:
        :return: bool or delete num
        """
        incr_key = self.incr_format.format(key=name)
        hash_key = self.hash_format.format(key=name)

        if start_timestamp or end_timestamp:
            if not start_timestamp:
                start_timestamp = "-inf"
            if not end_timestamp:
                end_timestamp = "+inf"
            result_data = self.client.zrangebyscore(name,
                                                    min=start_timestamp,
                                                    max=end_timestamp,
                                                    withscores=False)

            with self._pipe_acquire() as pipe:
                pipe.multi()
                pipe.decr(incr_key, len(result_data))
                pipe.zremrangebyscore(name, min=start_timestamp, max=end_timestamp)
                pipe.hdel(hash_key, *result_data)
                pipe.execute()
        else:
            return self.client.delete(name, incr_key, hash_key)

    def trim(self, name, length=1000):
        """
        trim redis sorted set key as the number of length,
        trim the data as the asc timestamp
        :param name:
        :param length:
        :return:
        """
        if length >= self.count(name):
            length = self.count(name)

        incr_key = self.incr_format.format(key=name)
        hash_key = self.hash_format.format(key=name)

        begin = 0
        end = length - 1

        result_data = self.client.zrange(name=name, start=begin, end=end, desc=False)

        if result_data:
            with self._pipe_acquire() as pipe:
                pipe.multi()
                pipe.decr(incr_key, length)
                pipe.zremrangebyrank(name, min=begin, max=end)
                pipe.hdel(hash_key, *result_data)
                pipe.execute()

    def get_slice(self, name, start=None, end=None,
                  start_index=None, limit=None, asc=True):
        """
        :param name:
        :param start:
        :param end:
        :param start_index:
        :param limit:
        :param asc:
        :return:
        """
        if asc:
            func = self.client.zrangebyscore
        else:
            func = self.client.zrevrangebyscore
        if start is None:
            start = "-inf"
        if end is None:
            end = "+inf"
        if start_index is None:
            start_index = 0

        if limit is None:
            limit = -1

        hash_key = self.hash_format.format(key=name)

        results_ids = func(name, min=start, max=end, withscores=True, start=start_index, num=limit)

        if results_ids:
            # sorted as the order data
            ids, timestamps = list(itertools.zip_longest(*results_ids))
            values = self.client.hmget(hash_key, *ids)
            iter_dumps = map(self.serializer.loads, values)
            return list(itertools.zip_longest(timestamps, iter_dumps))
        else:
            return []

    def remove_many(self, keys, *args, **kwargs):
        """
        remove many keys
        :param keys:
        :param args:
        :param kwargs:
        :return:
        """
        chunks_data = helper.chunks(keys, 10000)
        for chunk_keys in chunks_data:
            incr_chunks = map(lambda x: self.incr_format.format(key=x), chunk_keys)
            hash_chunks = map(lambda x: self.hash_format.format(key=x), chunk_keys)
            del_data = itertools.chain(chunk_keys, incr_chunks, hash_chunks)
            self.client.delete(*del_data)

    def add_many(self, name, timestamp_pairs, chunks_size=2000, *args, **kwargs):
        """
        :param name:
        :param timestamp_pairs: [("timestamp",data)]
        :param chunks_size:
        :param args:
        :param kwargs:
        :return:
        """
        incr_key = self.incr_format.format(key=name)
        hash_key = self.hash_format.format(key=name)

        # remove exist data

        # todo maybe other way to optimize this filter code
        sorted_timestamps = sorted(timestamp_pairs, key=itemgetter(0))

        max_timestamp = sorted_timestamps[-1][0]  # max
        min_timestamp = sorted_timestamps[0][0]  # min

        filter_data = self.get_slice(name, start=min_timestamp, end=max_timestamp)
        if filter_data:
            timestamp_set = set(map(lambda x: x[0], filter_data))
            filter_results = itertools.filterfalse(lambda x: x[0] in timestamp_set, sorted_timestamps)
        else:
            filter_results = sorted_timestamps
        chunks_data = helper.chunks(filter_results, chunks_size)

        with self._pipe_acquire() as pipe:
            for chunks in chunks_data:
                start_id = self.client.get(incr_key) or 1  # if key not exist id equal 0
                end_id = self.client.incrby(incr_key, amount=len(chunks))  # incr the add length

                start_id = int(start_id)
                end_id = int(end_id)

                ids_range = range(start_id, end_id)

                dumps_results = map(lambda x: (x[0], self.serializer.dumps(x[1])), chunks)

                mix_data = itertools.zip_longest(dumps_results, ids_range)  # [(("timestamp",data),id),...]
                mix_data = list(mix_data)  # need converted as list

                timestamp_ids = map(lambda seq: (seq[0][0], seq[1]), mix_data)  # [("timestamp",id),...]
                ids_pairs = map(lambda seq: (seq[1], seq[0][1]), mix_data)  # [("id",data),...]

                timestamp_ids = itertools.chain.from_iterable(timestamp_ids)
                ids_values = {k: v for k, v in ids_pairs}

                pipe.multi()
                pipe.zadd(name, *timestamp_ids)
                pipe.hmset(hash_key, ids_values)
                pipe.execute()

    def flush(self):
        """
        flush database
        :return:
        """
        self.client.flushdb()
