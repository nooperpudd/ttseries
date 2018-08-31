import unittest
import pandas
import datetime


class RedisPandasTimeSeries(unittest.TestCase):
    """
    """
    def prepare_dataframe(self, length):
        now = datetime.datetime.now()
        date_range = pandas.date_range(now, periods=length, freq="1min")

        data_frame = pandas.DataFrame([i + 1 for i in range(len(date_range))], index=date_range, columns=["value"])
        return data_frame

