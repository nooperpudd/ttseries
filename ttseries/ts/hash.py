# encoding:utf-8

import itertools

import ttseries.utils
from ttseries.ts.base import RedisTSBase


class RedisHashTimeSeries(RedisTSBase):
    """
    Use redis sorted sets and hashes to store time-series data,
    instead of using redis sorted sets, combine redis hashes with auto
    increase value, so the time-series can keep the data consistency.

    sorted sets [(timestamp,1),(timestamp,2),...]
    hashes [(1,data),(2,data)...]

    support max length 2**32-1 timestamp stored

    """
    hash_format = "{key}:HASH"  # as the hash set id
    incr_format = "{key}:ID"  # as the auto increase id

    def _auto_trim(self, name, key_id, hash_key):
        """
        auto trim the redis data  with max length in the redis keys
        :param name: redis key
        :param key_id:
        :param hash_key:
        """
        # if current length reach the max length of the data
        # remove oldest key store in data
        remove_key = key_id - self.max_length
        watch_keys = (name, hash_key)

        def pipe_func(_pipe):  # trans function
            self.client.zrem(name, remove_key)
            self.client.hdel(hash_key, remove_key)

        self.transaction_pipe(pipe_func, watch_keys)

    def get(self, name, timestamp):
        """
        get one item by timestamp
        :param name: redis key
        :param timestamp: float, timestamp
        :return: obj
        """
        hash_key = self.hash_format.format(key=name)

        result_id = self.client.zrangebyscore(name, min=timestamp, max=timestamp)
        if result_id:
            data = self.client.hmget(hash_key, result_id)
            return self._serializer.loads(data[0])

    def add(self, name: str, timestamp: float, data) -> bool:
        """
        add one times-series data into redis

        ensure only one timestamp corresponding one value
        :param name: key name
        :param timestamp: timestamp: float
        :param data: object
        :return: bool
        """
        self._validate_key(name)

        incr_key = self.incr_format.format(key=name)  # APPL:SECOND:ID
        hash_key = self.hash_format.format(key=name)  # APPL:second:HASH

        if not self.exist_timestamp(name, timestamp):

            dumps_data = self._serializer.dumps(data)

            key_id = self.client.incr(incr_key)  # int, key id start with 1
            # key id start with 1,2,3,4,5,6...

            if self.length(name) == self.max_length:
                self._auto_trim(name, key_id, hash_key)

            dumps_dict = {key_id: dumps_data}  # { 1: values}

            def pipe_func(_pipe):  # trans function
                _pipe.zadd(name, timestamp, key_id)  # APPL:SECOND, 233444334.33, 1
                _pipe.hmset(hash_key, dumps_dict)  # APPL:second:HASH, {1:value}

            watch_keys = (name, hash_key)  # APPL:SECOND , APPL:second:HASH

            return self.transaction_pipe(pipe_func, watch_keys)

    def delete(self, name, start_timestamp=None, end_timestamp=None):
        """
        Removes all elements in the sorted sets stored at key
        between start timestamp and end timestamp (inclusive).
        if parameter only contains `name`, will delete all data stored in redis key.

        :param name: redis key
        :param start_timestamp: timestamp
        :param end_timestamp: timestamp
        :return: bool or delete num
        """
        incr_key = self.incr_format.format(key=name)  # APPL:SECOND:ID
        hash_key = self.hash_format.format(key=name)  # APPL:second:HASH

        if start_timestamp or end_timestamp:

            if self.count(name, start_timestamp, end_timestamp) > 0:
                if not start_timestamp:
                    start_timestamp = "-inf"
                if not end_timestamp:
                    end_timestamp = "+inf"
                result_data = self.client.zrangebyscore(name,
                                                        min=start_timestamp,
                                                        max=end_timestamp,
                                                        withscores=False)

                watch_keys = (name, hash_key)

                def pipe_func(_pipe):
                    _pipe.zremrangebyscore(name, min=start_timestamp, max=end_timestamp)
                    _pipe.hdel(hash_key, *result_data)

                self.transaction_pipe(pipe_func, watch_keys)

        else:
            # redis delete command
            self.client.delete(name, incr_key, hash_key)

    def remove_many(self, names, start_timestamp=None, end_timestamp=None):
        """
        remove many keys with timestamp
        ! if only parameter contains names, will directly delete redis key.
        or with start timestamp and end timestamp will remove all elements
        in the sorted sets with keys, between with start timestamp and end timestamp

        :param names: tuple, redis keys
        :param start_timestamp: float, start timestamp
        :param end_timestamp: float, end timestamp
        """
        chunks_data = ttseries.utils.chunks(names, 1000)

        if start_timestamp or end_timestamp:
            for chunk_keys in chunks_data:
                for name in chunk_keys:
                    self.delete(name, start_timestamp, end_timestamp)
        else:
            for chunk_keys in chunks_data:
                incr_chunks = map(lambda x: self.incr_format.format(key=x), chunk_keys)
                hash_chunks = map(lambda x: self.hash_format.format(key=x), chunk_keys)
                del_items = itertools.chain(chunk_keys, incr_chunks, hash_chunks)
                self.client.delete(*del_items)

    def trim(self, name, length: int):
        """
        trim redis sorted sets key as the number of length,
        trim the data with timestamp as the asc
        :param name: redis key
        :param length: int, length
        """
        current_length = self.length(name)
        hash_key = self.hash_format.format(key=name)

        if current_length > length > 0:
            begin = 0  # start with 0 as the first set item
            end = length - 1

            result_data = self.client.zrange(name=name,
                                             start=begin,
                                             end=end, desc=False)
            if result_data:
                def pipe_func(_pipe):
                    _pipe.zremrangebyrank(name, min=begin, max=end)
                    _pipe.hdel(hash_key, *result_data)

                watch_keys = (name, hash_key)
                self.transaction_pipe(pipe_func, watch_keys)
        elif length >= current_length:
            self.delete(name)

    def get_slice(self, name, start_timestamp=None, end_timestamp=None,
                  limit=None, asc=True):
        """
        return a slice from redis sorted sets with timestamp pairs

        :param name: redis key
        :param start_timestamp: start timestamp
        :param end_timestamp: end timestamp
        :param limit: int, limit the length of the result data.
        :param asc: bool, sorted as the timestamp values
        :return: [(timestamp,data),...]
        """

        hash_key = self.hash_format.format(key=name)

        results_ids = self._get_slice_mixin(name, start_timestamp,
                                            end_timestamp, limit, asc)

        if results_ids:
            ids, timestamps = list(itertools.zip_longest(*results_ids))
            values = self.client.hmget(hash_key, *ids)
            iter_dumps = map(self._serializer.loads, values)
            return list(itertools.zip_longest(timestamps, iter_dumps))

    def add_many(self, name, timestamp_pairs, chunks_size=2000):
        """
        add large amount of data into redis sorted sets
        :param name: redis key
        :param timestamp_pairs: data pairs, [("timestamp",data)...]
        :param chunks_size: split data into chunk, optimize for redis pipeline
        """

        incr_key = self.incr_format.format(key=name)
        hash_key = self.hash_format.format(key=name)

        timestamp_pairs = self._add_many_validate_mixin(name, timestamp_pairs)

        chunks_data = ttseries.utils.chunks(timestamp_pairs, chunks_size)
        for chunks in chunks_data:

            if not self.client.exists(incr_key):
                self.client.incr(incr_key)

            start_id = self.client.get(incr_key)  # if key not exist id equal 0
            end_id = self.client.incrby(incr_key, amount=len(chunks))  # incr the add length

            ids_range = range(int(start_id), int(end_id))

            dumps_results = itertools.starmap(lambda timestamp, data:
                                              (timestamp, self._serializer.dumps(data)), chunks)

            mix_data = itertools.zip_longest(dumps_results, ids_range)
            mix_data = list(mix_data)
            # [(("timestamp",data),id),...]
            timestamp_ids = itertools.starmap(lambda timestamp_values, _id:
                                              (timestamp_values[0], _id), mix_data)  # [("timestamp",id),...]

            ids_pairs = itertools.starmap(lambda timestamp_values, _id:
                                          (_id, timestamp_values[1]), mix_data)  # [("id",data),...]

            timestamp_ids = itertools.chain.from_iterable(timestamp_ids)
            ids_values = {k: v for k, v in ids_pairs}

            def pipe_func(_pipe):
                _pipe.zadd(name, *tuple(timestamp_ids))
                _pipe.hmset(hash_key, ids_values)

            self.transaction_pipe(pipe_func, watch_keys=(name, hash_key))

    def iter_keys(self, count=None):
        """
        generator iterator all time-series keys
        :return: iter,
        """
        for item in self.client.scan_iter(match="*:ID", count=count):
            yield item.decode("utf-8").replace(":ID", "")

    def iter(self, name):
        """
        iterator all the time-series data with redis key.
        only works in python3.6,
        https://stackoverflow.com/questions/39980323/are-dictionaries-ordered-in-python-3-6
        :param name: redis key
        :return: iter, [(timestamp, data),...]
        """
        hash_key = self.hash_format.format(key=name)  # APPL:second:HASH

        for timestamp_pairs, hash_pairs in itertools.zip_longest(self.client.zscan_iter(name=name),
                                                                 self.client.hscan_iter(name=hash_key)):
            yield (timestamp_pairs[1], self._serializer.loads(hash_pairs[1]))
