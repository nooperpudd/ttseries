# encoding:utf-8
import contextlib
import itertools
from multiprocessing.pool import ThreadPool

import numpy as np

from .exceptions import RepeatedValueError


# @contextlib.contextmanager
def pool_map(func, iterable, chunk_size=1000):
    """
    Context Manager which return multiprocessing.pool apply func call calculation result.
    correctly deals with thrown exceptions.
    :param func:
    :param iterable:
    :param chunk_size:
    :return: list
    """
    pool = ThreadPool(2)
    try:
        return pool.map(func, iterable, chunk_size)
    except Exception as e:
        pool.terminate()
        raise e
    finally:
        pool.close()
        pool.join()





def check_array_repeated(array):
    """
    check array repeated keys, if exist repeated keys will raise RepeatedValueError
    :param array: [(key,value),....]
    :raise RepeatedValueError
    """
    keys, _ = itertools.zip_longest(*array)
    keys_dict = {}
    for key in keys:
        if key in keys_dict:
            raise RepeatedValueError("repeated value:", key)
        else:
            keys_dict.setdefault(key)


def chunks_np_or_pd_array(array, chunk_size: int = 2000):
    """
    split numpy array into chunk array
    :param array: numpy array
    :param chunk_size: int, split data as the length of chunks
    :return: yield numpy.ndarray
    """
    length = array.shape[0]
    if length > chunk_size:
        chunk = int(length / chunk_size)
        for item in np.array_split(array, chunk):
            yield item
    else:
        yield array


def np_datetime64_to_timestamp(dt64, decimals=6):
    """
    https://stackoverflow.com/questions/13703720/converting-between-datetime-timestamp-and-datetime64
    convert np.datetime64 to python datetime.timestamp
    :return: timestamp
    """
    value = (dt64 - np.datetime64("1970-01-01T00:00:00")) / np.timedelta64(1, 's')
    return float(np.around(value, decimals=decimals))


def chunks(iterable, chunk_size: int = 1000):
    """
    split iterable array into chunks

    >>> chunks(range(6),chunk_size=2)
    ... [(0,1),(2,3),(4,5)]
    :param iterable: list, iter
    :param chunk_size: chunk split size
    :return: yield iterable values
    """
    iter_data = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(iter_data, chunk_size))
        if not chunk:
            return
        yield chunk
