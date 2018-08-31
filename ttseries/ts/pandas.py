# encoding:utf-8

from ttseries.ts.sample import RedisSampleTimeSeries
from ttseries.exceptions import RedisTimeSeriesError

import pandas as pd
import ttseries.utils

import itertools
from collections import namedtuple


class RedisPandasTimeSeries(RedisSampleTimeSeries):
    """
    """

    def __init__(self, redis_client, max_length=100000,
                 columns=None,
                 *args, **kwargs):

        super(RedisPandasTimeSeries, self).__init__(redis_client=redis_client,
                                                    max_length=max_length, *args, **kwargs)

        self.columns = columns
        self._row_name_tuple = namedtuple("data", columns)

    def _timestamp_exist(self, name, data_frame):
        """
        :param name:
        :param array:
        :return:
        """
        date_index = data_frame.index

        start_timestamp = data_frame.idxmin()
        end_timestamp = data_frame.idxmax()

        exist_length = self.count(name, start_timestamp, end_timestamp)

        if exist_length > 0:
            timestamps_dict = {item: None for item in date_index}

            filer_array = self.get_slice(name, start_timestamp, end_timestamp)

            if self.dtype:
                filter_timestamps = filer_array[self.timestamp_column_name]
            else:
                filter_timestamps = filer_array[:, self.timestamp_column_index]

            for timestamp in filter_timestamps:
                if timestamp in timestamps_dict:
                    raise RedisTimeSeriesError("add duplicated timestamp into redis -> timestamp:", timestamp)

    def _auto_trim_array(self, name, array_data):
        """
        :param name:
        :param array_data:
        :return:
        """
        length = array_data.size
        # auto trim array
        if length + self.length(name) >= self.max_length:
            trim_length = length + self.length(name) - self.max_length
            self.trim(name, trim_length)

        if length > self.max_length:
            array_data = array_data.iloc[length - self.max_length:]
        return array_data

    def add_many(self, name, data_frame, chunks_size=2000):
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
        if type(data_frame.index) is not pd.DatetimeIndex:
            raise RedisTimeSeriesError("DataFrame index must be pandas.DateTimeIndex type")
        # auto trim timestamps
        array = self._auto_trim_array(name, data_frame)
        # validate timestamp exist
        self._timestamp_exist(name, array)
        for chunk_array in ttseries.utils.chunks_np_or_pd_array(array, chunks_size):

            with self._lock, self._pipe_acquire() as pipe:
                pipe.watch(name)
                pipe.multi()

                # To preserve dtypes while iterating over the rows, it is better
                # to use :meth:`itertuples` which returns namedtuples of the values
                # and which is generally faster than ``iterrows``

                for row in chunk_array.itertuples():
                    index = row[0]  # index
                    row_data = row[1:]  # reset data tuple
                    pipe.zadd(name, index.timestamp(), self._serializer.dumps(row_data))
                pipe.execute()
    def add(self, name: str, timestamp: float, data):
        """
        :param name:
        :param timestamp:
        :param data:
        :return:
        """
        pass

    def get(self, name: str, timestamp: float):
        """
        get one item by timestamp
        :param name:
        :param timestamp:
        :return: numpy.ndarray
        """
        data = super().get(name, timestamp)
        if data:
            return pd.Series(data,timestamp,name=self.columns)

    def iter(self, name, count=None):
        """
        iterable all data with count values
        :param name: redis key
        :param count: the count length of records
        :return: yield numpy.ndarray
        """
        for timestamp, data in super().iter(name,count):
            yield pd.Series(data,timestamp,name=self.columns)

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

            data = itertools.starmap(lambda values: (values[1], self._serializer.loads(values[0])),results)

            values = itertools.starmap()
            return
            # if self.dtype is None:
            #     column_index = self.timestamp_column_index
            #
            #     def apply_numpy_index(serializer_data, timestamp):
            #         data = self._serializer.loads(serializer_data)
            #         data.insert(column_index, timestamp)
            #         return data
            #
            #     values = itertools.starmap(apply_numpy_index, results)
            #     return np.array(list(values))
            #
            # else:
            #     column_index = self.timestamp_names_index
            #
            #     def apply_numpy_column(serializer_data, timestamp):
            #         data = self._serializer.loads(serializer_data)
            #         data.insert(column_index, timestamp)
            #         return tuple(data)
            #
            #     values = itertools.starmap(apply_numpy_column, results)
            #     return np.fromiter(values, dtype=self.dtype)
