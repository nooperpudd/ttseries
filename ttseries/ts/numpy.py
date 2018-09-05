# encoding:utf-8
import itertools

import numpy as np

import ttseries.utils
from ttseries.exceptions import RedisTimeSeriesError
from .sample import RedisSampleTimeSeries


class RedisNumpyTimeSeries(RedisSampleTimeSeries):
    """
    Numpy TimeSeries support Numpy array with dtype or
    just assign the timestamp column index
    """

    def __init__(self, redis_client, max_length=100000,
                 dtype=None,
                 timestamp_column_name=None,
                 timestamp_column_index=0,
                 *args, **kwargs):
        """
        :param dtype: numpy.dtype, if set the dtype and timestamp_column_name can't be None
        :param timestamp_column_name: timestamp column name
        :param timestamp_column_index: timestamp column index
        :param args:
        :param kwargs:
        """
        super(RedisNumpyTimeSeries, self).__init__(redis_client=redis_client,
                                                   max_length=max_length, *args, **kwargs)

        if dtype is not None and timestamp_column_name is None:
            raise RedisTimeSeriesError("dtype and timestamp_column_name "
                                       "must both be specified")

        if dtype:
            self.dtype = np.dtype(dtype)
            self.timestamp_column_name = timestamp_column_name

            self.names = list(self.dtype.names)
            self.timestamp_name_index = self.names.index(timestamp_column_name)
        else:
            self.dtype = None

        self.timestamp_column_index = timestamp_column_index

    def _check_repeated_timestamp_index(self, array):
        """
        sorted timestamp and check exist repeated timestamp
        :param array:
        :return:
        """
        # sort timestamp
        if self.dtype:
            timestamp_array = array[self.timestamp_column_name].astype("float64")
            array[self.timestamp_column_name] = timestamp_array
            array = np.sort(array, order=[self.timestamp_column_name])
        else:
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

            filer_array = self.get_slice(name, start_timestamp, end_timestamp)

            if self.dtype:
                filter_timestamps = filer_array[self.timestamp_column_name]
            else:
                filter_timestamps = filer_array[:, self.timestamp_column_index]

            for timestamp in filter_timestamps:
                if timestamp in timestamps_dict:
                    raise RedisTimeSeriesError("add duplicated timestamp into redis -> timestamp:", timestamp)

    def add_many(self, name, array: np.ndarray, chunks_size=2000):
        """
        add large amount of numpy array into redis
        >>>[[timestamp,"a","c"],
        >>> [timestamp,"b","e"],
        >>> [timestamp,"c","a"],...]
        :param name: redis key
        :param array: numpy.ndarray
        :param chunks_size: int, split data into chunk, optimize for redis pipeline
        """
        self._validate_key(name)

        array = self._check_repeated_timestamp_index(array)
        # auto trim timestamps
        array = self._auto_trim_array(name, array)
        # validate timestamp exist
        self._timestamp_exist(name, array)

        for chunk_array in ttseries.utils.chunks_np_or_pd_array(array, chunks_size):

            if self.dtype:
                timestamp_index = self.names.index(self.timestamp_column_name)
            else:
                timestamp_index = self.timestamp_column_index

            def iter_numpy(*row):
                timestamp = row[timestamp_index]
                list_data = row[:timestamp_index] + row[timestamp_index + 1:]  # tuple add
                list_data = tuple(np.asscalar(item) for item in list_data)
                data = self._serializer.dumps(list_data)
                return timestamp, data

            data_pairs = itertools.starmap(iter_numpy, chunk_array)
            data_chains = itertools.chain.from_iterable(data_pairs)

            def pipe_func(_pipe):
                _pipe.zadd(name, *tuple(data_chains))

            self.transaction_pipe(pipe_func, watch_keys=name)

    def get(self, name: str, timestamp: float):
        """
        get one item by timestamp
        :param name:
        :param timestamp:
        :return: numpy.ndarray
        """
        data = super(RedisNumpyTimeSeries, self).get(name, timestamp)

        if data:
            if self.dtype is None:
                data.insert(self.timestamp_column_index, timestamp)
                return np.array(data)
            else:
                data.insert(self.timestamp_name_index, timestamp)
                return np.array(tuple(data), dtype=self.dtype)

    def iter(self, name, count=None):
        """
        iterable all data with count values
        :param name: redis key
        :param count: the count length of records
        :return: yield numpy.ndarray
        """

        if self.dtype is None:
            for timestamp, data in super(RedisNumpyTimeSeries, self).iter(name, count):
                data.insert(self.timestamp_column_index, timestamp)
                yield np.array(data)
        else:

            for timestamp, data in super(RedisNumpyTimeSeries, self).iter(name, count):
                data.insert(self.timestamp_name_index, timestamp)
                yield np.array(tuple(data), dtype=self.dtype)

    def get_slice(self, name, start_timestamp=None,
                  end_timestamp=None, limit=None, asc=True):
        """
        return a slice numpy array from redis sorted sets

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
                data.insert(column_index, timestamp)
                return tuple(data)

            values = itertools.starmap(apply_numpy_index, results)

            if self.dtype is None:
                column_index = self.timestamp_column_index
                return np.array(list(values))
            else:

                column_index = self.timestamp_name_index
                return np.fromiter(values, dtype=self.dtype)
