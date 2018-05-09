# encoding:utf-8
import numpy as np
import redis
from ttseries.ts.base import RedisTSBase
import itertools

class RedisSampleTimeSeries(RedisTSBase):
    """
    ! important
    """

    def _auto_trim(self):
        pass

    def add(self, name: str, timestamp, data):
        data = self._serializer.dumps(data)

        with self._lock:
            if self.length(name) > self.max_length:
                pass
            else:
                return self.client.zadd(name, timestamp, data)

    def add_many(self, name, array: np.array):
        array_length = len(array)

        if array_length + self.length(name) > self.max_length:
            pass

        def pipe_func(_pipe):
            pass

        self.transaction_pipe(pipe_func, watch_keys=name)

    def get(self, name: str, timestamp):
        result = self.client.zrangebyscore(name, min=timestamp,
                                           max=timestamp)
        return self._serializer.loads(result)

    def delete(self, name: str, start_timestamp=None, end_timestamp=None):

        if start_timestamp or end_timestamp:
            if start_timestamp is None:
                start_timestamp = "-inf"
            if end_timestamp is None:
                end_timestamp = "+inf"
            self.client.zremrangebyscore(name, min=start_timestamp, max=end_timestamp)
        else:
            self.client.delete(name)

    def iter(self, name):
        pass

    def trim(self,name, length):
        """

        :param name:
        :param length:
        :return:
        """
        current_length = self.length(name)

        if current_length > length:
            begin = 0  # start with 0 as the first set item
            end = length - 1

            self.client.zremrangebyrank(name, min=begin, max=end)

        else:
            self.delete(name)

    def get_slice(self, name, start_timestamp=None,
                  end_timestamp=None,limit=None,asc=True):

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

        results_ids = zrange_func(name, min=start_timestamp,
                                  max=end_timestamp,
                                  withscores=True, start=0, num=limit)

        if results_ids:
            # todo fix this
            value, timestamps = list(itertools.zip_longest(*results_ids))
            iter_dumps = map(self._serializer.loads, value)
            return list(itertools.zip_longest(timestamps, iter_dumps))



