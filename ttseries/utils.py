# encoding:utf-8
import itertools

import numpy as np


def chunks_numpy(array: np.ndarray, chunk_size: int = 2000) -> np.ndarray:
    """
    split numpy array in to chunk data
    :param array: numpy array
    :param chunk_size: int, split data as the length of chunks
    :return: numpy array
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
