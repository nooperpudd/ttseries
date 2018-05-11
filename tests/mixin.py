class Mixin(object):

    def generate_data(self, length):
        data_list = []
        for i in range(length):
            timestamp = self.timestamp + i
            data = {"value": i}
            data_list.append((timestamp, data))
        return data_list

    def prepare_data(self):

        key = "APPL:SECOND:1"
        results = self.generate_data(10)
        for item in results:
            self.time_series.add(key, item[0], item[1])
        return key

    def add_data_list(self, data):

        key = "APPL:SECOND:1"
        for item in data:
            self.time_series.add(key, item[0], item[1])
        return key

    def prepare_many_data(self, data_list):

        keys = ["APPL:DAY:" + str(key) for key in range(20)]
        for key in keys:
            for timestamp, item in data_list:
                self.time_series.add(key, timestamp, item)
        return keys

    def test_add(self):
        key = "APPL:SECOND:1"

        data = {
            "value": 21,
            "volume": 11344.34,
            "asks": [{"ask_1": 10, "price": 21}],
            "bid": [{"bid_1": 10, "price": 20}]
        }

        result = self.time_series.add(key, self.timestamp, data)
        self.assertTrue(result)

    def test_add_repeated_timestamp(self):
        """
        ensure each timestamp mapping to each value
        """
        key = "APPL:SECOND:3"
        data = {"value": 21}
        data2 = {"value": 33}
        result = self.time_series.add(key, self.timestamp, data)
        result2 = self.time_series.add(key, self.timestamp, data2)
        self.assertTrue(result)
        self.assertFalse(result2)

    def test_add_max_length(self):

        key = self.prepare_data()

        new_data = {"value": 10}
        new_data2 = {"value": 11}
        timestamp = self.timestamp + 10
        timestamp2 = self.timestamp + 11

        self.time_series.add(key, timestamp=timestamp, data=new_data)
        self.time_series.add(key, timestamp=timestamp2, data=new_data2)

        self.assertEqual(self.time_series.get(key, self.timestamp), None)
        self.assertEqual(self.time_series.get(key, self.timestamp + 1), None)
        self.assertEqual(self.time_series.get(key, self.timestamp + 2), {"value": 2})
        self.assertEqual(self.time_series.get(key, self.timestamp + 10), {"value": 10})
        self.assertEqual(self.time_series.get(key, self.timestamp + 11), {"value": 11})

    def test_count(self):

        key = self.prepare_data()
        self.assertEqual(self.time_series.count(key), 10)

    def test_count_with_timestamp(self):

        key = self.prepare_data()
        start_timestamp = self.timestamp + 5
        self.assertEqual(self.time_series.count(key, start_timestamp), 5)

    def test_length(self):
        key = self.prepare_data()
        self.assertEqual(self.time_series.length(key), 10)

    def test_get(self):

        key = "APPL:SECOND:4"
        data = {"value": 10.3}
        self.time_series.add(key, self.timestamp, data)
        result = self.time_series.get(key, self.timestamp)
        self.assertDictEqual(data, result)

    def test_delete_one_timestamp(self):
        """
        test delete with only one timestamp
        :return:
        """
        key = "APPL:SECOND:6"
        data = {"value": 10.3}
        self.time_series.add(key, self.timestamp, data)
        self.time_series.delete(key, start_timestamp=self.timestamp,
                                end_timestamp=self.timestamp)
        self.assertEqual(self.time_series.count(key), 0)
        self.assertFalse(self.time_series.exist_timestamp(key, self.timestamp))

    def test_delete_with_start_timestamp(self):
        """
        test delete with start timestamp
        :return:
        """
        data_list = self.generate_data(10)
        key = self.add_data_list(data_list)
        start_timestamp = self.timestamp + 3  # delete from start timestamp

        self.time_series.delete(key, start_timestamp=start_timestamp)
        self.assertEqual(self.time_series.count(key), 3)

        for item in data_list[:]:
            if item[0] >= start_timestamp:
                data_list.remove(item)
                result = self.time_series.get(key, item[0])
                self.assertEqual(result, None)

        for time, item in data_list:
            result = self.time_series.get(key, time)
            self.assertEqual(result, item)

    def test_delete_with_end_timestamp(self):
        data_list = self.generate_data(10)

        key = self.add_data_list(data_list)

        end_timestamp = self.timestamp + 5

        self.time_series.delete(key, end_timestamp=end_timestamp)
        self.assertEqual(self.time_series.count(key), 4)

        for item in data_list[:]:
            if item[0] <= end_timestamp:
                data_list.remove(item)
                result = self.time_series.get(key, item[0])
                self.assertEqual(result, None)

        for time, item in data_list:
            result = self.time_series.get(key, time)
            self.assertEqual(result, item)

    def test_delete_with_start_and_end(self):
        data_list = self.generate_data(10)

        key = self.add_data_list(data_list)

        start_timestamp = self.timestamp + 3
        end_timestamp = self.timestamp + 6

        self.time_series.delete(key, start_timestamp=start_timestamp,
                                end_timestamp=end_timestamp)
        self.assertEqual(self.time_series.count(key), 6)

        for item in data_list[:]:
            if start_timestamp <= item[0] <= end_timestamp:
                data_list.remove(item)
                result = self.time_series.get(key, item[0])
                self.assertEqual(result, None)

        for time, item in data_list:
            result = self.time_series.get(key, time)
            self.assertEqual(result, item)

    def test_delete_with_max_length_auto_trim(self):
        """
        :return:
        """
        data_list = self.generate_data(10)

        key = self.add_data_list(data_list)

        new_data = {"value": 12}
        new_data2 = {"value": 13}
        new_timestamp = self.timestamp + 10
        new_timestamp2 = self.timestamp + 11

        self.time_series.add(key, new_timestamp, new_data)
        self.time_series.add(key, new_timestamp2, new_data2)

        data_list.append((new_timestamp, new_data))
        data_list.append((new_timestamp2, new_data2))

        data_list.pop(0)  # remove data list first item
        data_list.pop(0)

        start_timestamp = self.timestamp + 8

        self.time_series.delete(name=key, start_timestamp=start_timestamp)

        for item in data_list[:]:
            if item[0] >= start_timestamp:
                data_list.remove(item)
                result = self.time_series.get(key, item[0])
                self.assertEqual(result, None)
        for time, item in data_list:
            result = self.time_series.get(key, time)
            self.assertEqual(result, item)

    def test_delete_with_max_length_trim_start_and_end_timestamp(self):
        """
        :return:
        """
        data_list = self.generate_data(10)

        key = self.add_data_list(data_list)

        new_data = {"value": 12}
        new_data2 = {"value": 13}
        new_timestamp = self.timestamp + 10
        new_timestamp2 = self.timestamp + 11

        self.time_series.add(key, new_timestamp, new_data)
        self.time_series.add(key, new_timestamp2, new_data2)

        data_list.append((new_timestamp, new_data))
        data_list.append((new_timestamp2, new_data2))

        data_list.pop(0)  # remove data list first item
        data_list.pop(0)

        start_timestamp = self.timestamp + 8
        end_timestamp = self.timestamp + 10
        self.time_series.delete(name=key,
                                start_timestamp=start_timestamp,
                                end_timestamp=end_timestamp)

        for item in data_list[:]:
            if start_timestamp <= item[0] <= end_timestamp:
                data_list.remove(item)
                result = self.time_series.get(key, item[0])
                self.assertEqual(result, None)
        for time, item in data_list:
            result = self.time_series.get(key, time)
            self.assertEqual(result, item)

    def test_remove_many_with_start_end_timestamp(self):

        data_list = self.generate_data(10)

        keys = self.prepare_many_data(data_list)

        start_timestamp = self.timestamp + 5
        end_timestamp = self.timestamp + 8

        self.time_series.remove_many(keys, start_timestamp, end_timestamp)

        for item in data_list[:]:
            if start_timestamp <= item[0] <= end_timestamp:
                data_list.remove(item)
                for key in keys:
                    result = self.time_series.get(key, item[0])
                    self.assertEqual(result, None)

    def test_trim(self):

        data_list = self.generate_data(10)

        key = self.add_data_list(data_list)

        self.time_series.trim(key, 5)

        self.assertEqual(self.time_series.length(key), 5)

        data_list = sorted(data_list, key=lambda k: k[0])

        result_data_list = data_list[5 - len(data_list):]

        result_data = self.time_series.get_slice(key)

        self.assertListEqual(result_data_list, result_data)

        trim_data_list = data_list[:5]

        for timestamp, _ in trim_data_list:
            self.assertIsNone(self.time_series.get(key, timestamp))

    def test_trim_lgt_max_length(self):

        data_list = self.generate_data(10)

        key = self.add_data_list(data_list)

        self.time_series.trim(key, 20)

        self.assertEqual(self.time_series.length(key), 0)

    def test_get_slice_with_key(self):

        data_list = self.generate_data(10)
        key = self.add_data_list(data_list)

        result_data = self.time_series.get_slice(key)
        self.assertListEqual(data_list, result_data)

    def test_get_slice_with_key_desc(self):
        data_list = self.generate_data(10)
        key = self.add_data_list(data_list)
        result_data = self.time_series.get_slice(key, asc=False)

        reversed_data = sorted(data_list, key=lambda tup: tup[0], reverse=True)

        self.assertListEqual(reversed_data, result_data)

    def test_get_slice_with_start_timestamp(self):
        """
        test get slice with start timestamp
        :return:
        """
        data_list = self.generate_data(10)

        key = self.add_data_list(data_list)

        start = self.timestamp + 6
        result_data = self.time_series.get_slice(key, start_timestamp=start)

        filter_data = list(filter(lambda seq: seq[0] >= start, data_list))

        self.assertEqual(result_data, filter_data)

    def test_get_slice_with_gt_start_timestamp(self):

        data_list = self.generate_data(10)
        key = self.add_data_list(data_list)

        start = self.timestamp + 6
        start_timestamp = "(" + str(start)
        result_data = self.time_series.get_slice(key, start_timestamp=start_timestamp)

        filter_data = list(filter(lambda seq: seq[0] > start, data_list))

        self.assertEqual(result_data, filter_data)

    def test_get_slice_with_start_timestamp_limit(self):

        data_list = self.generate_data(10)

        key = self.add_data_list(data_list)

        start = self.timestamp + 5
        result_data = self.time_series.get_slice(key, start_timestamp=start, limit=3)

        filter_data = list(filter(lambda seq: seq[0] >= start, data_list))
        filter_data = filter_data[:3]

        self.assertEqual(result_data, filter_data)

    def test_get_slice_with_start_timestamp_desc(self):

        data_list = self.generate_data(10)

        key = self.add_data_list(data_list)

        start = self.timestamp + 6
        start_timestamp = "(" + str(start)
        result_data = self.time_series.get_slice(key,
                                                 start_timestamp=start_timestamp, asc=False)

        filter_data = list(filter(lambda seq: seq[0] > start, data_list))
        filter_data = list(reversed(filter_data))
        self.assertEqual(result_data, filter_data)

    def test_get_slice_with_end_timestamp(self):
        """
        test get slice only with end timestamp
        :return:
        """
        data_list = self.generate_data(10)
        key = self.add_data_list(data_list)

        end = self.timestamp + 6

        result_data = self.time_series.get_slice(key, end_timestamp=end)
        filter_data = list(filter(lambda seq: seq[0] <= end, data_list))
        self.assertEqual(result_data, filter_data)

    def test_get_slice_with_timestamp(self):

        data_list = self.generate_data(10)

        key = self.add_data_list(data_list)

        start = self.timestamp + 6
        end = self.timestamp + 10
        result_data = self.time_series.get_slice(key, start, end)
        filter_data = list(filter(lambda seq: start <= seq[0] <= end, data_list))
        self.assertEqual(result_data, filter_data)

    def test_get_slice_with_start_length(self):
        data_list = self.generate_data(10)

        key = self.add_data_list(data_list)

        result_data = self.time_series.get_slice(key, limit=5)
        filter_data = data_list[:5]
        self.assertEqual(result_data, filter_data)

    def test_get_slice_start_end_time_order(self):

        data_list = self.generate_data(10)
        key = self.add_data_list(data_list)

        start_timestamp = self.timestamp + 3
        end_timestamp = self.timestamp + 6

        result_data = self.time_series.get_slice(key,
                                                 start_timestamp=start_timestamp,
                                                 end_timestamp=end_timestamp,
                                                 asc=False)

        data_list = data_list[3:7]
        filter_data = list(reversed(data_list))
        self.assertListEqual(result_data, filter_data)
