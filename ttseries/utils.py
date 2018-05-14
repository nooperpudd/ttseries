# encoding:utf-8
import itertools
import multiprocessing
from multiprocessing.pool import ThreadPool

import numpy as np


def p_map(func, iterable, chunk_size=1000):
    """
    optimize map func
    :param func:
    :param iterable:
    :param chunk_size:
    :return: list
    """
    pool = ThreadPool(multiprocessing.cpu_count())
    return pool.map(func, iterable, chunk_size)


def chunks_numpy(array: np.array, chunk_size=2000):
    """
    :return:
    """
    length = len(array)
    if length > chunk_size:
        chunk = int(length / chunk_size)
        for item in np.array_split(array, chunk):
            yield item
    else:
        yield array


def chunks(iterable, chunk_size=1000):
    """
    >>> chunks(range(6),chunk_size=2)
    ... [(0,1),(2,3),(4,5)]
    :param iterable: list, iter
    :param chunk_size: chunk split size
    :return: yield, iter
    """
    iter_data = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(iter_data, chunk_size))
        if not chunk:
            return
        yield chunk
