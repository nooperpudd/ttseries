# encoding:utf-8
import itertools
from datetime import datetime

import pandas as pd

import ttseries.utils
from ttseries.exceptions import RedisTimeSeriesError
from ttseries.ts.sample import RedisSampleTimeSeries


class RedisPandasTimeSeries(RedisSampleTimeSeries):
    """
    """

    def __init__(self, redis_client, timezone, columns,
                 index_name=None,
                 dtypes=None, max_length=100000,
                 *args, **kwargs):
        """
        :param redis_client:
        :param timezone:
        :param columns:
        :param dtypes:
        :param max_length:
        :param args:
        :param kwargs:
        """

        super(RedisPandasTimeSeries, self).__init__(redis_client=redis_client,
                                                    max_length=max_length, *args, **kwargs)

        self.columns = columns
        self.tz = timezone
        self.dtypes = dtypes
        self.index_name = index_name

    def _timestamp_exist(self, name, data_frame):
        """
        :param name:
        :param data_frame:
        :return:
        """
        date_index = data_frame.index

        start_timestamp = data_frame.idxmin()
        start_timestamp = start_timestamp.value.timestamp()

        end_timestamp = data_frame.idxmax()
        end_timestamp = end_timestamp.value.timestamp()
        exist_length = self.count(name, start_timestamp, end_timestamp)

        if exist_length > 0:
            timestamps_dict = {item: None for item in date_index}
            filer_dataframe = self.get_slice(name, start_timestamp, end_timestamp)

            filter_timestamps = filer_dataframe.index

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
        :param data_frame: pandas.DataFrame
        :param chunks_size: int, split data into chunk, optimize for redis pipeline
        """
        self._validate_key(name)
        if not isinstance(data_frame.index, pd.DatetimeIndex):
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

    def add(self, name, series):
        """
        :param name:
        :param series:
        :return: bool
        """
        self._validate_key(name)
        if isinstance(series, pd.Series) and hasattr(series.name, "timestamp"):
            timestamp = series.name.timestamp()

            with self._lock:
                if not self.exist_timestamp(name, timestamp):
                    values = series.tolist()
                    data = self._serializer.dumps(values)
                    if self.length(name) == self.max_length:
                        # todo use 5.0 BZPOPMIN
                        self.client.zremrangebyrank(name, min=0, max=0)
                    return self.client.zadd(name, timestamp, data)
        else:
            raise RedisTimeSeriesError("Please check series Type or "
                                       "series name value is not pandas.DateTimeIndex type")

    def get(self, name: str, timestamp: float):
        """
        get one item by timestamp
        :param name:
        :param timestamp:
        :return: pandas.Series
        """
        data = super(RedisPandasTimeSeries, self).get(name, timestamp)
        if data:
            date = datetime.fromtimestamp(timestamp, tz=self.tz)
            return pd.Series(data=data, index=self.columns, name=date)

    def iter(self, name, count=None):
        """
        iterable all data with count values
        :param name: redis key
        :param count: the count length of records
        :return: yield numpy.ndarray
        """
        for timestamp, data in super(RedisPandasTimeSeries, self).iter(name, count):
            date = datetime.fromtimestamp(timestamp, tz=self.tz)
            yield pd.Series(data=data, index=self.columns, name=date)

    def get_slice(self, name, start_timestamp=None,
                  end_timestamp=None, limit=None, asc=True):
        """
        return a slice numpy array from redis sorted sets

        :param name: redis key
        :param start_timestamp: start timestamp
        :param end_timestamp: end timestamp
        :param limit: int,
        :param asc: bool, sorted as the timestamp values
        :return:
        """

        results = self._get_slice_mixin(name, start_timestamp,
                                        end_timestamp, limit, asc)

        if results:
            # [(b'\x81\xa5value\x00', 1526008483.331131),...]

            data = list(itertools.starmap(lambda serializer, timestamp:
                                          [datetime.fromtimestamp(timestamp, tz=self.tz)] +
                                          self._serializer.loads(serializer),
                                          results))
            # todo optmise in future
            data_frame = pd.DataFrame.from_records(data, index=[0])
            data_frame.index.names = [self.index_name]
            data_frame = data_frame.drop(axis=1, columns=[0])
            data_frame.columns = self.columns
            if self.dtypes:
                data_frame = data_frame.astype(dtype=self.dtypes)
            return data_frame
