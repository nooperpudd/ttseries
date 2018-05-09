# encoding:utf-8
import itertools
import numpy as np

def chunks_numpy(array:np.array,chunk_size=2000):
    """
    :return:
    """
    length = len(array)
    if length > chunk_size:
        chunk = int(length/chunk_size)
        for item in np.array_split(array,chunk):
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
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, chunk_size))
        if not chunk:
            return
        yield chunk
