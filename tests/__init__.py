
class TestMixin(object):

    def generate_data(self, length):
        data_list = []
        for i in range(length):
            timestamp = self.timestamp + i
            data = {"value": i}
            data_list.append((timestamp, data))
        return data_list
