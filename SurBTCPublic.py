import requests


def _ms2sec(ts_str):
    return int(int(ts_str) / 1000)


class SurBtcPublic:
    def __init__(self, market_id):
        self.base_url = "https://www.buda.com/api/v2/markets"
        Markets.check(market_id)
        self.market_id = market_id

    def get_market_state(self):
        url = self.base_url + "/{}/ticker.json".format(self.market_id)
        response = requests.get(url)
        if response.status_code != 200:
            raise ConnectionError("Código de error {}".format(response.status_code))
        return response.json()

    def get_transactions(self, since_ts):
        since_ms = int(since_ts * 1000)  # converts seconds to milliseconds
        url = self.base_url + "/{}/trades.json?timestamp={}".format(self.market_id, since_ms)
        response = requests.get(url)

        if response.status_code != 200:
            raise ConnectionError("Código de error {}".format(response.status_code))

        # return response.json()
        return SurBtcTransactions(response.json())


class Markets:
    BTC = "btc-clp"
    ETH = "eth-clp"
    BCH = "bch-clp"

    @staticmethod
    def check(market_str):
        valid_market_list = ["btc-clp", "eth-clp", "eth-btc", "bch-clp", "bch-btc"]
        if market_str not in valid_market_list:
            raise KeyError("El código del mercado especificado no existe")


class TransactionEntry:
    TIMESTAMP = 0
    AMOUNT = 1
    PRICE = 2
    DIRECTION = 3

    def __init__(self, entry):
        self.timestamp = _ms2sec(entry[self.TIMESTAMP])
        self.amount = float(entry[self.AMOUNT])
        self.price = float(entry[self.PRICE])
        self.direction = entry[self.DIRECTION]

    def __str__(self):
        return "timestamp: {}\namount: {}\nprice: {}\ndirection: {}\n".format(self.timestamp,
                                                                              self.amount,
                                                                              self.price,
                                                                              self.direction)


class SurBtcTransactions:
    def __init__(self, json):
        trades = json["trades"]
        self.timestamp = _ms2sec(trades["timestamp"])
        self.last_timestamp = _ms2sec(trades["last_timestamp"])
        self.entries = list(map(lambda x: TransactionEntry(x), trades["entries"]))
        self._entries_len = len(self.entries)
        self._i = 0

    def __str__(self):
        dict = {}
        dict["timestamp"] = self.timestamp
        dict["last_timestamp"] = self.last_timestamp
        dict["entries"] = self.entries
        return str(dict)

    def __iter__(self):
        self._i = 0
        return self

    def __next__(self):
        if self._i < self._entries_len:
            entry = self.entries[self._i]
            self._i += 1
            return entry
        else:
            self._i = 0
            raise StopIteration()

    def __reversed__(self):
        return reversed(self.entries)