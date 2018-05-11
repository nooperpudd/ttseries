# encoding:utf-8

import numpy as np

import ttseries.utils
from ttseries.ts.base import RedisTSBase


class RedisSampleTimeSeries(RedisTSBase):
    """
    ! important
    """

    def add(self, name: str, timestamp, data):

        data = self._serializer.dumps(data)

        with self._lock:
            if self.length(name) >= self.max_length:
                self.client.zremrangebyrank(name, min=0, max=0)
            if not self.exist_timestamp(name, timestamp):
                return self.client.zadd(name, timestamp, data)

    def add_many(self, name, array: np.array, chunk_size=1000):
        """
        array data likes: [[1,"a"],[2,"b"],[3,"c"],...]
        :param name:
        :param array:
        :return:
        """
        array_length = len(array)

        if array_length + self.length(name) > self.max_length:
            trim_length = array_length + self.length(name) - self.max_length
            self.trim(name, trim_length)
        # todo check exists
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
        current_length = self.length(name)
        with self._lock:
            if current_length > length:
                begin = 0  # start with 0 as the first set item
                end = length - 1
                self.client.zremrangebyrank(name, min=begin, max=end)

            else:
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

            return list(map(lambda x: (x[1], self._serializer.loads(x[0])),
                            results))
