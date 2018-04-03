# encoding:utf-8
import itertools


def chunks(iterable, chunk_size=1000):
    """
    >>> chunks(range(6),n=2)
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
