import os


class CsvPersistor:
    def __init__(self, path):
        self.save_path = path
        self.market = None

    def persist(self):
        assert self.market is not None
        save_path = os.path.join(os.path.realpath(self.save_path), self.market + '.csv')
        line_list = []

        if os.path.isfile(save_path):
            with open(save_path) as file:
                line_list = file.read().split('\n')
        else:
            pass

