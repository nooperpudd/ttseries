import itertools

import numpy as np

import ttseries.utils
from ttseries.exceptions import RedisTimeSeriesError
from .sample import RedisSampleTimeSeries


class RedisNumpyTimeSeries(RedisSampleTimeSeries):

    def __init__(self, dtype=None, *args, **kwargs):
        """
        :param dtype: numpy dtype,
        :param args:
        :param kwargs:
        """
        super().__init__(*args, **kwargs)
        self.dtype = dtype  # numpy data dtype

    def _add_many_validate(self, array,
                           timestamp_column_name=None,
                           timestamp_column_index=0):

        """
        :param array:
        :param timestamp_column_name:
        :param timestamp_column_index:
        :return:
        """
        if timestamp_column_name:
            # sort timestamp
            timestamp_array = array[timestamp_column_name].astype("float64")
            array[timestamp_column_name] = timestamp_array
            array = np.sort(array, order=[timestamp_column_name])

        else:
            # sort timestamp
            timestamp_array = array[:, timestamp_column_index].astype("float64")
            array[:, timestamp_column_index] = timestamp_array
            array = np.sort(array, axis=timestamp_column_index)

        # check repeated
        if len(np.unique(timestamp_array)) != len(timestamp_array):
            raise RedisTimeSeriesError("repeated timestamps in array data")

        return array

    def _timestamp_exist(self, name, array,
                         timestamp_column_name=None,
                         timestamp_column_index=0):
        """
        :param name:
        :param array:
        :param timestamp_column_name:
        :param timestamp_column_index:
        :return:
        """
        if timestamp_column_name:
            timestamp_array = array[timestamp_column_name]
        else:
            timestamp_array = array[:, timestamp_column_index]

        start_timestamp = timestamp_array.min()
        end_timestamp = timestamp_array.max()

        exist_length = self.count(name, start_timestamp, end_timestamp)

        if exist_length > 0:

            timestamps_dict = {item: None for item in timestamp_array}

            array = self.get_slice(name, start_timestamp, end_timestamp)

            filter_timestamps, _ = itertools.zip_longest(*array)
            for timestamp in filter_timestamps:
                if timestamp in timestamps_dict:
                    raise RedisTimeSeriesError("add duplicated timestamp into redis -> timestamp:", timestamp)

    def add_many(self, name, array: np.ndarray,
                 timestamp_column_index=0,
                 timestamp_column_name=None,
                 chunk_size=1000):
        """
        array data likes:
        >>>[[timestamp,"a","c"],
        >>> [timestamp,"b","e"],
        >>> [timestamp,"c","a"],...]
        :param name:
        :param array:
        :param timestamp_column_index:
        :param timestamp_column_name:
        :param chunk_size:
        :return:
        """

        self._validate_key(name)

        array = self._add_many_validate(array, timestamp_column_name, timestamp_column_index)
        # auto trim timestamps
        array = self._auto_trim_array(name, array)
        # validate timestamp exist
        self._timestamp_exist(name, array, timestamp_column_name, timestamp_column_index)

        for item in ttseries.utils.chunks_numpy(array, chunk_size):
            with self._lock, self._pipe_acquire() as pipe:
                pipe.watch(name)
                pipe.multi()

                def iter_numpy(arr):
                    timestamp = arr[0]
                    data = self._serializer.dumps(arr[1:].tolist())
                    pipe.zadd(name, timestamp, data)

                np.apply_along_axis(iter_numpy, 1, item)

                pipe.execute()

    def get(self, name: str, timestamp: float):
        """
        :param name:
        :param timestamp:
        :return:
        """
        data = super().get(name, timestamp)
        array = [timestamp].extend(data)
        if self.dtype is None:
            return np.array(array)
        else:
            return np.array(array, dtype=self.dtype)

    def iter(self, name, count=None):
        """
        :param name:
        :param count:
        :return:
        """
        for timestamp, data in super().iter(name, count):
            array = [timestamp].extend(data)
            if self.dtype:
                yield np.array(array)
            else:
                yield np.array(array, dtype=self.dtype)

    def get_slice(self, name, start_timestamp=None,
                  end_timestamp=None, limit=None, asc=True):
        """
        return a slice from redis sorted sets with timestamp pairs

        :param name: redis key
        :param start_timestamp: start timestamp
        :param end_timestamp: end timestamp
        :param limit: int,
        :param asc: bool, sorted as the timestamp values
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

        if limit is None:
            limit = -1

        results = zrange_func(name, min=start_timestamp, max=end_timestamp,
                              withscores=True,
                              start=0, num=limit)

        if results:
            pass
