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
