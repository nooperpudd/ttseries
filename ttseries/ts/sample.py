# encoding:utf-8

import itertools

import numpy as np

import ttseries.utils
from ttseries.ts.base import RedisTSBase
from ttseries.utils import p_map


class RedisSampleTimeSeries(RedisTSBase):
    """
    ! important
    """

    def add(self, name: str, timestamp, data):

        data = self._serializer.dumps(data)

        with self._lock:
            if not self.exist_timestamp(name, timestamp):
                if self.length(name) == self.max_length:
                    self.client.zremrangebyrank(name, min=0, max=0)
                return self.client.zadd(name, timestamp, data)

    def add_many(self, name, timestamp_pairs, chunks_size=2000):
        """

        :param name:
        :param timestamp_pairs:
        :param chunks_size:
        :return:
        """

        timestamp_pairs = self._add_many_validate(name, timestamp_pairs)

        for item in ttseries.utils.chunks(timestamp_pairs, chunks_size):
            filter_data = p_map(lambda x: (x[0], self._serializer.dumps(x[1])), item)
            filter_data = itertools.chain.from_iterable(filter_data)

            def pipe_func(_pipe):
                _pipe.zadd(name, *tuple(filter_data))

            self.transaction_pipe(pipe_func, watch_keys=name)

    def add_many_with_numpy(self, name, array, timestamp_name="timestamp", chunk_size=1000):
        """
        array data likes: [[1,"a"],[2,"b"],[3,"c"],...]
        :param name:
        :param array:
        :param timestamp_name:
        :return:
        """
        self._add_many_validate(name, array)

        # array[:, 1::]
        serializer_func = np.vectorize(self._serializer.dumps)
        for item in ttseries.utils.chunks_numpy(array, 1000):

            for inner in item:
                def pipe_func(_pipe):
                    _pipe.zadd(name, *inner.tolist())

                self.transaction_pipe(pipe_func, watch_keys=name)

    def get(self, name: str, timestamp):
        """
        :param name:
        :param timestamp:
        :return:
        """
        result = self.client.zrangebyscore(name, min=timestamp, max=timestamp)
        if result:
            return self._serializer.loads(result[0])

    def delete(self, name: str, start_timestamp=None, end_timestamp=None):

        with self._lock:
            if start_timestamp or end_timestamp:
                if start_timestamp is None:
                    start_timestamp = "-inf"
                if end_timestamp is None:
                    end_timestamp = "+inf"
                self.client.zremrangebyscore(name, min=start_timestamp, max=end_timestamp)
            else:
                self.client.delete(name)

    def remove_many(self, names, start_timestamp=None, end_timestamp=None):
        """
        remove many keys
        :param names:
        :param start_timestamp:
        :param end_timestamp:
        :return:
        """
        chunks_data = ttseries.utils.chunks(names, 10000)

        if start_timestamp or end_timestamp:
            for chunk_keys in chunks_data:
                for name in chunk_keys:
                    self.delete(name, start_timestamp, end_timestamp)
        else:
            for chunk_keys in chunks_data:
                self.client.delete(*chunk_keys)

    def iter(self, name):
        pass

    def trim(self, name, length):
        """

        :param name:
        :param length:
        :return:
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
                  end_timestamp=None, limit=None, asc=True):
        """

        :param name:
        :param start_timestamp:
        :param end_timestamp:
        :param limit:
        :param asc:
        :return:
        """
        if asc:
            zrange_func = self.client.zrangebyscore
        else:
            zrange_func = self.client.zrevrangebyscore
        if start_timestamp is None:
            start_timestamp = "-inf"
        if end_timestamp is None:
            end_timestamp = "+inf"

        if limit is None:
            limit = -1

        results = zrange_func(name, min=start_timestamp,
                              max=end_timestamp,
                              withscores=True, start=0, num=limit)

        if results:
            # [(b'\x81\xa5value\x00', 1526008483.331131),...]

            return p_map(lambda x: (x[1], self._serializer.loads(x[0])), results)
