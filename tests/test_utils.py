# encoding:utf-8
import datetime
import unittest

import numpy as np

from ttseries.exceptions import RepeatedValueError
from ttseries.utils import chunks, chunks_np_or_pd_array, check_array_repeated


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

    def test_chunks_numpy_with_large(self):
        array_rand_0 = np.random.rand(10, 2)

        result_array = list(chunks_np_or_pd_array(array_rand_0, 20))
        self.assertTrue(np.array_equal(array_rand_0, result_array[0]))

        array_rand_1 = np.random.rand(20, 2)
        split_array = np.array_split(array_rand_1, 2)
        array_1, array_2 = split_array
        result_array_1, result_array_2 = list(chunks_np_or_pd_array(array_rand_1, 10))
        self.assertTrue(np.array_equal(array_1, result_array_1))
        self.assertTrue(np.array_equal(array_2, result_array_2))

        array_rand_2 = np.random.rand(5, 2)
        array_3, array_4 = np.array_split(array_rand_2, 2)

        result_array_3, result_array_4 = list(chunks_np_or_pd_array(array_rand_2, 2))
        self.assertTrue(np.array_equal(array_3, result_array_3))
        self.assertTrue(np.array_equal(array_4, result_array_4))

    def test_check_timestamp_repeat(self):
        now = datetime.datetime.now()
        timestamp = now.timestamp()
        test_data = []
        for i in range(5):
            test_data.append((timestamp + i, i))
        test_data.append((timestamp, 10))

        with self.assertRaises(RepeatedValueError):
            check_array_repeated(test_data)
