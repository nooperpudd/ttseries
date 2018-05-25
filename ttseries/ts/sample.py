# encoding:utf-8

import itertools

import ttseries.utils
from ttseries.ts.base import RedisTSBase


class RedisSampleTimeSeries(RedisTSBase):
    """
    Redis Time-series storage based on Sorted set.

    !! important: Redis sorted sets can't create uniqueness item,
    for example: if we want to store different timestamp with the
    same data value, redis will ignore the data we want to try to store
    it in the sorted sets.

    >>>(1526611599.240008, "a")
    >>>(1526613549.240008, "a") # this item will not store it.

    In-order to avoid this condition, user must avoid the repeated data to
    store in the redis sorted sets.
    If it's not important for the duplicated data to be stored in the redis,
    we can just use RedisSampleTimeSeries, or use RedisHashTimeSeries instead of it.

    but we still could has an option to change the source code of the redis,
    let's it support some specific patch to full support time-series data.
    """

    def add(self, name: str, timestamp: float, data):
        """
        add one times-series data into redis
        :param name: redis key
        :param timestamp: float
        :param data: obj
        :return: int
        """
        self._validate_key(name)
        with self._lock:
            if not self.exist_timestamp(name, timestamp):
                data = self._serializer.dumps(data)
                if self.length(name) == self.max_length:
                    self.client.zremrangebyrank(name, min=0, max=0)
                return self.client.zadd(name, timestamp, data)

    def add_many(self, name, timestamp_pairs: list, chunks_size=2000):
        """
        add large amount of data into redis sorted sets
        :param name: redis key
        :param timestamp_pairs: data pairs, [("timestamp",data)...]
        :param chunks_size: split data into chunk, optimize for redis pipeline
        """
        timestamp_pairs = self._add_many_validate_mixin(name, timestamp_pairs)

        for item in ttseries.utils.chunks(timestamp_pairs, chunks_size):
            filter_data = itertools.starmap(lambda timestamp, data:
                                            (timestamp, self._serializer.dumps(data)), item)
            filter_data = itertools.chain.from_iterable(filter_data)

            def pipe_func(_pipe):
                _pipe.zadd(name, *tuple(filter_data))

            self.transaction_pipe(pipe_func, watch_keys=name)

    def get(self, name: str, timestamp: float):
        """
        get one item by timestamp
        :param name: redis key
        :param timestamp: float, timestamp
        :return: obj
        """
        result = self.client.zrangebyscore(name, min=timestamp, max=timestamp)
        if result:
            return self._serializer.loads(result[0])

    def delete(self, name: str, start_timestamp=None, end_timestamp=None):
        """
        Removes all elements in the sorted sets stored at key
        between start timestamp and end timestamp (inclusive).
        if parameter only contains `name`, will delete all data stored in redis key.

        :param name: redis key,
        :param start_timestamp: start timestamp
        :param end_timestamp: end timestamp
        :return: int, the result of the elements removed
        """
        with self._lock:
            if start_timestamp or end_timestamp:
                if start_timestamp is None:
                    start_timestamp = "-inf"
                if end_timestamp is None:
                    end_timestamp = "+inf"
                return self.client.zremrangebyscore(name, min=start_timestamp, max=end_timestamp)
            else:
                return self.client.delete(name)

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
                self.client.delete(*chunk_keys)

    def trim(self, name, length):
        """
        trim the redis sorted sets as the length of the data.
        trim the data with timestamp as the asc
        :param name: redis key
        :param length: int, length
        """
        length = int(length)
        current_length = self.length(name)

        with self._lock:
            if current_length > length > 0:
                begin = 0  # start with 0 as the first set item
                end = length - 1
                self.client.zremrangebyrank(name, min=begin, max=end)
            elif length >= current_length:
                self.delete(name)

    def get_slice(self, name, start_timestamp=None,
                  end_timestamp=None, limit=None, asc=True, chunks_size=10000):
        """
        return a slice from redis sorted sets with timestamp pairs

        chunk_size>=total>=limit  -> set limit = num
        total>limit>chunk_size -> set limit = num, iter data
        total>chunk_size>limit -> set limit = num


        limit>total>chunk_size -> total, set limit = -1
        limit>chunk_size>total -> total, set limit = -1  = > limit > total
        chunk_size>limit>total -> total, set limit = -1


        :param name: redis key
        :param start_timestamp: start timestamp
        :param end_timestamp: end timestamp
        :param limit: int,
        :param asc: bool, sorted as the timestamp values
        :param chunks_size: int, yield chunk size iter data.
        :return: [(timestamp,data),...]
        """
        if asc:
            zrange_func = self.client.zrangebyscore
        else:  # desc
            zrange_func = self.client.zrevrangebyscore

        if start_timestamp is None:
            start_timestamp = "-inf"
        if end_timestamp is None:
            end_timestamp = "+inf"

        total = self.count(name, start_timestamp, end_timestamp)

        def zrange_func_limit_(start_, num_):

            results = zrange_func(name, min=start_timestamp,
                                  max=end_timestamp,
                                  withscores=True,
                                  start=start_, num=num_)

            yield list(itertools.starmap(lambda data, timestamp:
                                         (timestamp, self._serializer.loads(data)),
                                         results))

        if total > 0:
            if limit is None or limit >= total:
                if total > chunks_size:
                    split = int(total / chunks_size) + 1

                    for i in range(split):  # start with 0,1,2,3...
                        if i == 0:
                            start = 0
                        else:
                            start = index + 1

                        yield_data = yield zrange_func_limit_(start, chunks_size)

                        index_data = self._serializer.dumps(yield_data[-1])
                        index = self.client.zrank(name, index_data)
                else:
                    yield zrange_func_limit_(start_=0, num_=-1)
            else:  # limit < total
                if limit > chunks_size:

                    split = int(limit / chunks_size) + 1
                    for i in range(split):
                        if i == 0:
                            start = 0
                            num = chunks_size
                        elif i == split - 1:  # the last
                            start = index + 1
                            num = limit - chunks_size * i
                        else:
                            num = chunks_size
                            start = index + 1

                        yield_data = yield zrange_func_limit_(start, num)

                        index_data = self._serializer.dumps(yield_data[-1])
                        index = self.client.zrank(name, index_data)
                else:
                    yield zrange_func_limit_(start_=0, num_=limit)

    def iter_keys(self, count=None):
        """
        generator iterator all time-series keys
        :param count: the number of the keys
        :return: iter,
        """
        for item in self.client.scan_iter(count=count):
            yield item.decode("utf-8")

    def iter(self, name, count=None):
        """
        iterator all the time-series data with redis key.
        :param name: redis key
        :param count:
        :return: iter, [(timestamp, data),...]
        """
        for item in self.client.zscan_iter(name, count=count):
            # ( timestamp, array_data)
            yield (item[1], self._serializer.loads(item[0]))
