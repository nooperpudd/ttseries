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
                    # todo use 5.0 BZPOPMIN
                    self.client.zremrangebyrank(name, min=0, max=0)

                return self.client.zadd(name, mapping={data: timestamp})

    def add_many(self, name, array: list, chunks_size=2000):
        """
        add large amount of data into redis sorted sets
        :param name: redis key
        :param array: data pairs, [("timestamp",data)...]
        :param chunks_size: split data into chunk, optimize for redis pipeline
        """
        timestamp_pairs = self._add_many_validate_mixin(name, array)

        for item in ttseries.utils.chunks(timestamp_pairs, chunks_size):

            result_data = {self._serializer.dumps(data): timestamp for timestamp, data in item}

            def pipe_func(_pipe):
                _pipe.zadd(name, result_data)

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

    def trim(self, name: str, length: int):
        """
        trim the redis sorted sets as the length of the data.
        trim the data with timestamp as the asc
        :param name: redis key
        :param length: int, length
        """
        current_length = self.length(name)

        with self._lock:
            if current_length > length > 0:
                begin = 0  # start with 0 as the first set item
                end = length - 1
                self.client.zremrangebyrank(name, min=begin, max=end)
            elif length >= current_length:
                self.delete(name)

    def get_slice(self, name, start_timestamp=None,
                  end_timestamp=None, limit=None, asc=True):
        """
        return a slice from redis sorted sets with timestamp pairs

        :param name: redis key
        :param start_timestamp: start timestamp
        :param end_timestamp: end timestamp
        :param limit: int, limit the length of the result data.
        :param asc: bool, sorted as the timestamp values
        :return: [(timestamp,data),...]
        """

        results = self._get_slice_mixin(name, start_timestamp,
                                        end_timestamp, limit, asc)
        if results:
            # [(b'\x81\xa5value\x00', 1526008483.331131),...]
            return list(itertools.starmap(lambda data, timestamp:
                                          (timestamp, self._serializer.loads(data)),
                                          results))

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
            # (timestamp, array_data)
            yield (item[1], self._serializer.loads(item[0]))
