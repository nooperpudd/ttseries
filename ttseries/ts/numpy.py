import itertools

import numpy as np

import ttseries.utils
from ttseries.exceptions import RedisTimeSeriesError
from .sample import RedisSampleTimeSeries


class RedisNumpyTimeSeries(RedisSampleTimeSeries):

    def __init__(self, redis_client, max_length=100000,
                 dtype=None,
                 timestamp_column_name=None,
                 timestamp_column_index=0,
                 *args, **kwargs):
        """
        :param dtype: numpy dtype,
        :param timestamp_column_name:
        :param timestamp_column_index:
        :param args:
        :param kwargs:
        """
        super().__init__(redis_client=redis_client,
                         max_length=max_length, *args, **kwargs)

        if dtype is not None and timestamp_column_name is None:
            raise RedisTimeSeriesError("dtype and timestamp_column_name "
                                       "must both be specified")

        if dtype:
            self.dtype = np.dtype(dtype)
            self.timestamp_column_name = timestamp_column_name
        else:
            self.dtype = None

        self.timestamp_column_index = timestamp_column_index

    def _add_many_validate(self, array):

        """
        :param array:
        :return:
        """
        if self.dtype:
            # sort timestamp
            timestamp_array = array[self.timestamp_column_name].astype("float64")
            array[self.timestamp_column_name] = timestamp_array
            array = np.sort(array, order=[self.timestamp_column_name])

        else:
            # sort timestamp
            timestamp_array = array[:, self.timestamp_column_index].astype("float64")
            array[:, self.timestamp_column_index] = timestamp_array
            array = np.sort(array, axis=self.timestamp_column_index)

        # check repeated
        if len(np.unique(timestamp_array)) != len(timestamp_array):
            raise RedisTimeSeriesError("repeated timestamps in array data")

        return array

    def _timestamp_exist(self, name, array):
        """
        :param name:
        :param array:
        :return:
        """
        if self.dtype:
            timestamp_array = array[self.timestamp_column_name]
        else:
            timestamp_array = array[:, self.timestamp_column_index]

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

    def add_many(self, name, array: np.ndarray, chunk_size=1000):
        """
        array data likes:
        >>>[[timestamp,"a","c"],
        >>> [timestamp,"b","e"],
        >>> [timestamp,"c","a"],...]
        :param name:
        :param array:
        :param chunk_size:
        :return:
        """

        self._validate_key(name)

        array = self._add_many_validate(array)
        # auto trim timestamps
        array = self._auto_trim_array(name, array)
        # validate timestamp exist
        self._timestamp_exist(name, array)

        for chunk_array in ttseries.utils.chunks_numpy(array, chunk_size):
            with self._lock, self._pipe_acquire() as pipe:
                pipe.watch(name)
                pipe.multi()

                if self.dtype:
                    names = list(chunk_array.dtype.names)
                    names.remove(self.timestamp_column_name)

                    for row in chunk_array:
                        timestamp = row[self.timestamp_column_name]
                        data = row[names].tolist()
                        serializer_data = self._serializer.dumps(data)
                        pipe.zadd(name, timestamp, serializer_data)
                else:

                    def iter_numpy(arr):
                        timestamp_ = arr[self.timestamp_column_index]
                        part_one = arr[:self.timestamp_column_index].tolist()
                        part_two = arr[self.timestamp_column_index + 1:].tolist()
                        data_ = part_one + part_two
                        serializer_data_ = self._serializer.dumps(data_)
                        pipe.zadd(name, timestamp_, serializer_data_)

                    np.apply_along_axis(iter_numpy, 1, chunk_array)

                pipe.execute()

    def get(self, name: str, timestamp: float):
        """
        :param name:
        :param timestamp:
        :return: numpy.ndarray
        """
        data = super().get(name, timestamp)

        if data:
            if self.dtype is None:
                data.insert(self.timestamp_column_index, timestamp)
                return np.array(data)
            else:
                names = list(self.dtype.names)
                timestamp_index = names.index(self.timestamp_column_name)
                data.insert(timestamp_index, timestamp)

                return np.array(tuple(data), dtype=self.dtype)

    def iter(self, name, count=None):
        """
        :param name:
        :param count:
        :return: iter, numpy.ndarray
        """

        if self.dtype is None:
            for timestamp, data in super().iter(name, count):
                data.insert(self.timestamp_column_index, timestamp)
                yield np.array(data)
        else:
            names = list(self.dtype.names)
            timestamp_index = names.index(self.timestamp_column_name)

            for timestamp, data in super().iter(name, count):
                data.insert(timestamp_index, timestamp)
                yield np.array(tuple(data), dtype=self.dtype)

    def get_slice(self, name, start_timestamp=None,
                  end_timestamp=None, limit=None, asc=True):
        """
        return a slice from redis sorted sets with timestamp pairs

        :param name: redis key
        :param start_timestamp: start timestamp
        :param end_timestamp: end timestamp
        :param limit: int,
        :param asc: bool, sorted as the timestamp values
        :return: numpy.ndarray
        """

        results = self._get_slice_mixin(name, start_timestamp,
                                        end_timestamp, limit, asc)

        if results:
            # [(b'\x81\xa5value\x00', 1526008483.331131),...]

            def apply_numpy_index(serializer_data, timestamp):
                data = self._serializer.loads(serializer_data)
                data.insert(self.timestamp_column_index, timestamp)
                return data

            def apply_numpy_column(serializer_data, timestamp):
                data = self._serializer.loads(serializer_data)
                data.insert(timestamp_index, timestamp)
                return tuple(data)

            if self.dtype is None:
                values = itertools.starmap(apply_numpy_index, results)
                return np.array(list(values))

            else:
                names = list(self.dtype.names)
                timestamp_index = names.index(self.timestamp_column_name)
                values = itertools.starmap(apply_numpy_column, results)
                return np.fromiter(values, dtype=self.dtype)
