# encoding:utf-8
import itertools
import multiprocessing
from multiprocessing.pool import Pool
import contextlib

import numpy as np

from .exceptions import RepeatedValueError


@contextlib.contextmanager
def pool_map(func, iterable, chunk_size=1000):
    """
    Context Manager which return multiprocessing.pool apply func call calculation result.
    correctly deals with thrown exceptions.
    :param func:
    :param iterable:
    :param chunk_size:
    :return: list
    """
    with Pool(multiprocessing.cpu_count()) as pool:
        try:
            yield pool.starmap(func, iterable, chunk_size)
            pool.close()
        except Exception as e:
            pool.terminate()
            raise e
        finally:
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


def chunks_numpy(array: np.ndarray, chunk_size: int = 2000) -> np.ndarray:
    """
    split numpy array into chunk array
    :param array: numpy array
    :param chunk_size: int, split data as the length of chunks
    :return: yield numpy.ndarray
    """
    length = len(array)
    if length > chunk_size:
        chunk = int(length / chunk_size)
        for item in np.array_split(array, chunk):
            yield item
    else:
        yield array


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
