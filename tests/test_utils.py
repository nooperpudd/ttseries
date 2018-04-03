# encoding:utf-8
import unittest

from ttseries.utils import chunks


class ChunksTest(unittest.TestCase):
    def test_chunks(self):
        chunk_data = chunks(range(6), 2)
        chunk_data = list(chunk_data)
        self.assertEqual(chunk_data, [(0, 1), (2, 3), (4, 5)])

    def test_one_chunk(self):
        chunk_data = chunks(range(2), 5)
        chunk_data = list(chunk_data)
        self.assertEqual(chunk_data, [(0, 1)])

    def test_less_chunk(self):
        chunk_data = chunks(range(1), 6)
        chunk_data = list(chunk_data)
        self.assertEqual(chunk_data, [(0,)])
