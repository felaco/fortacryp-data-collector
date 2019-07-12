import datetime
import logging
import os
from typing import Optional, Dict, Union

import pandas as pd

logger = logging.getLogger('FortacrypLogger')


class KrakenHistoricalDataBase:
    def __init__(self, market: str):
        available_markets = ('btc', 'ltc', 'bch', 'eth')
        if market not in available_markets:
            raise KeyError('Market {} is not a valid market. Market list: {}'.format(market, available_markets))

        self.market = market
        self.base_path: str = './'
        self.csv_name: str = 'cryptoCompare_{}.csv'
        self.data: Optional[pd.DataFrame] = None
        self.open = None
        self.high = None
        self.low = None
        self.volume = 0
        self.has_open = False
        self.logger = logger

    def load_data(self) -> None:
        path = self._get_save_path(self.market)
        if not os.path.isfile(path):
            raise FileNotFoundError('Historical data file {} does not exist'.format(path))

        self.data = pd.read_csv(path)

    def append(self, dict_data: Dict[str, Union[float, int]]) -> pd.DataFrame:
        if not isinstance(self.data, pd.DataFrame):
            raise TypeError('Attribute Data of KrakenHistoricalDataBase is not DataFrame type.')

        ohlc = {
            'open': [dict_data['open'], ],
            'high': [dict_data['high'], ],
            'low': [dict_data['low'], ],
            'close': [dict_data['close'], ],
            'volumefrom': [dict_data['volume'], ]
        }

        last_timestamp = dict_data['last_timestamp_socket']
        now = int(datetime.datetime.now().replace(minute=0, second=0, microsecond=0).timestamp())
        if abs(now - last_timestamp) > 3600:
            raise ValueError('Attempting to store a timestamp ({}) that is inconsistent with datetime.now()'
                             .format(last_timestamp))

        last_timestamp = datetime.datetime.fromtimestamp(last_timestamp).replace(minute=0, second=0, microsecond=0)
        last_timestamp = int(last_timestamp.timestamp())
        ohlc['time'] = [last_timestamp, ]
        new_row = pd.DataFrame(ohlc)

        dataframe = self.data
        dataframe = dataframe.append(new_row, sort=False)
        # reorders the columns
        # https://stackoverflow.com/a/23741480
        # dataframe = dataframe[['time', 'open', 'high', 'low', 'close', 'volumefrom']]

        self.data = dataframe
        return dataframe

    def append_ticket(self, ticket: Dict[str, float]):
        if not isinstance(self.data, pd.DataFrame):
            raise TypeError('Attribute Data of KrakenHistoricalDataBase is not DataFrame type.')

        if self._should_apped_new(ticket['timestamp']):
            self._insert_new_ohlc(ticket)
        else:
            if not self.has_open:
                self._reinit_state_on_opening_price(ticket)
            else:
                self._update_on_new_price(ticket)

    def persist(self):
        self.logger.info('Persisting dataframe to csv. With tail')
        self.logger.info(self.data.tail())
        self.data.to_csv(self._get_save_path(self.market))

    def _should_apped_new(self, timestamp) -> None:
        last_stored = float(self.data['time'].values[-1])
        return timestamp - last_stored > 3600

    def _insert_new_ohlc(self, ticket: Dict[str, float]) -> None:
        new_candle = {
            'open': self.open,
            'high': self.high,
            'close': ticket['price'],
            'low': self.low,
            'volume': self.volume
        }
        self.logger.info('New OHLC: {}'.format(new_candle))
        self.append(new_candle)
        self.has_open = False
        self.persist()

    def _reinit_state_on_opening_price(self, ticket: Dict[str, float]) -> None:
        self.logger.info('New Opening price: {}'.format(ticket['price']))
        self.has_open = True
        self.open = ticket['price']
        self.high = self.open
        self.low = self.open
        self.volume = 0

    def _update_on_new_price(self, ticket: Dict[str, float]) -> None:
        self.high = ticket['price'] if ticket['price'] > self.high else self.high
        self.low = ticket['price'] if ticket['price'] < self.low else self.low
        self.volume += ticket['volume']

    def _get_save_path(self, market):
        return os.path.join(self.base_path, self.csv_name.format(market))


class BaseKrakenTicketHandler:
    def __init__(self):
        self.available_markets = ('btc', 'eth', 'ltc', 'bch')
        self.market_data: Dict[str, KrakenHistoricalDataBase] = {}
        self.logger = logger

    def init_open_data(self, market, open_price, high, low, volume):
        self._verify_market(market)
        self.market_data[market].open = float(open_price)
        self.market_data[market].high = float(high)
        self.market_data[market].low = float(low)
        self.market_data[market].volume = float(volume)
        self.market_data[market].has_open = True
        self.logger.info('Price init for market: {}. Open:'
                         ' {} Low: {} High: {} Volume: {}'.format(market, open_price, low, high, volume))

        now = int(datetime.datetime.now().timestamp())
        last_timestamp = int(self.market_data[market].data['time'].values[-1])

        if now - last_timestamp > 3600:
            raise ValueError('{}: Has not the updated data. Updated data is data with less than 3600 seconds'
                             ' of delay with now(). Last stored timestamp: {}, now: {}'.
                             format(market, last_timestamp, now))

    def on_new_ticket(self, ticket: Dict[str, Union[str, float]]) -> None:
        self._verify_market(ticket['market'])
        self.market_data[ticket['market']].append_ticket(ticket)

    def _verify_market(self, market) -> None:
        if market not in self.available_markets:
            raise KeyError('Market {} is not a valid market. Market list: {}'.format(market, self.available_markets))

        if market not in self.market_data.keys():
            data = KrakenHistoricalDataBase(market)
            data.load_data()
            self.market_data[market] = data
