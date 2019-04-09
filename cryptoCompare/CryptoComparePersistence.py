import os


def _tick_to_line(ticks: list):
    res = []
    for tick in ticks:
        res.append('{},{},{},{},{},{}'.format(
            tick['time'],
            tick['open'],
            tick['high'],
            tick['low'],
            tick['close'],
            tick['volumefrom']
        ))
    return res


def _merge_prepend(ticks, stored):
    """
    merge the saved data in a csv file with the new data from a request to crypto compare.
    The new data is prepended to the csv data, since it is considered older than the stored.
    This function makes no validation on this.
    :param ticks: new data from crypto compare, must be older than the stored. Should be a list of
    dict with at least the keys: time, open, high, low, close
    :param stored: A list of strings representing the stored data in a csv format. Each element
    is separated by a comma char, just like in every csv.
    :return:
    """

    if ticks is not None and len(ticks) > 0:
        ticks = _tick_to_line(ticks)
        stored_to_file = ['time, open, high, low, close, volumefrom']
        stored_to_file.extend(ticks[:-1])  # should not store the last ticket, since it is contained in
        # the stored list
        stored_to_file.extend(stored[1:])
        return stored_to_file
    else:
        return stored


def _merge_append(ticks, stored):
    """
    Merge the saved data in a csv file with new data from a request to crypto compare. The new data
    is appended to the ending of the list of stored data, since it is suposed to be newer.
    The new ticket data is filtered using their timestamp. If it is older than the last stored, it is
    filtered.
    :param ticks: new data. Should be newer than the stored data
    :param stored: A list of stored data in csv format each line
    :return: merged list
    """

    stored_to_file = stored
    # remember: stored is a list of strings in csv format, so i take the last element and the first
    # column as the last timestamp
    if ticks is not None and len(ticks) > 0:
        last_timestamp = int(stored[-1].split(',')[0])
        filtered_ticks = list(filter(lambda tick: tick['time'] > last_timestamp, ticks))
        stored_to_file.extend(_tick_to_line(filtered_ticks))
    return stored_to_file


def _merge_stored_with_recovered_lists(ticks: list, stored: list):
    if len(ticks) == 0:
        return stored

    # index 0 is the header, so data is available from index 1 onward
    first_line = stored[1].split(',')
    last_tick = ticks[-1]

    if int(first_line[0]) >= int(last_tick['time']):  # index 0 is the time col in the csv
        return _merge_prepend(ticks, stored)
    else:
        return _merge_append(ticks, stored)


def _save_list(l: list, path: str):
    with open(path, 'w') as file:
        file.writelines(map(lambda line: line + '\n', l))


class CsvPersistor:
    """
    A persistor for crypto compare integration. It should store new data in correct order in some way.
    In this case csvPersistor stores in a csv file, either prepending or appending new data from current
    file. You can add other persistor, its only requisite is to have a persistor method wich receives a list.
    ... And maybe change its constructor, but you can do better
    """

    def __init__(self, path):
        self.save_path = path
        self.market = None

    def set_market(self, market):
        self.market = market

    def persist(self, entry_list: list):
        if self.market is None:
            raise AttributeError('market attribute of the instance should not be None')

        save_path = os.path.join(os.path.realpath(self.save_path), 'cryptoCompare_' + self.market + '.csv')

        if os.path.isfile(save_path):
            with open(save_path) as file:
                # readlines function doesnt split with each line!!.
                line_list = file.read().split('\n')
                line_list = _merge_stored_with_recovered_lists(entry_list, line_list)

        else:
            line_list = _tick_to_line(entry_list)

        _save_list(line_list, save_path)
