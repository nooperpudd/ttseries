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
        :param timestamp_column_name: str, timestamp numpy column name
        :param timestamp_column_index: int, timestamp numpy column index

        :param name:
        :param array_data:
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
        :param data_array:
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

            for array in self.get_slice(name, start_timestamp, end_timestamp):

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
                  end_timestamp=None, limit=None, asc=True, chunks_size=10000):
        """
        return a slice from redis sorted sets with timestamp pairs

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
        # total, limit , chunk_size

        if limit is not None and total >= limit > chunks_size:
            pass
        if total > chunks_size:

            split_size = int(total / chunks_size)

            for i in range(split_size):

                if i == 0:
                    start = 0
                else:
                    start = index + 1

                results = zrange_func(name, min=start_timestamp, max=end_timestamp,
                                      withscores=True,
                                      start=start, num=chunks_size)

                if self.use_numpy:
                    pass
                else:
                    yield_data = yield list(itertools.starmap(lambda data, timestamp:
                                                              (timestamp, self._serializer.loads(data)),
                                                              results))

                    index_data = self._serializer.dumps(yield_data[-1])
                    index = self.client.zrank(name, index_data)

        else:

            # if limit is not None and limit<chunks_size:
            #     pass
            # elif:

            results = zrange_func(name, min=start_timestamp,
                                  max=end_timestamp,
                                  withscores=True, start=0, num=-1)

            # [(b'\x81\xa5value\x00', 1526008483.331131),...]
            if self.use_numpy:
                pass
            else:
                yield list(itertools.starmap(lambda data, timestamp:
                                             (timestamp, self._serializer.loads(data)),
                                             results))
