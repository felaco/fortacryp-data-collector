import datetime
import json
import logging
import os
import threading
import time
import requests
import krakenWebSocket.KrakenConstants as constants

import pandas as pd
from websocket import create_connection

from cryptoCompare.CryptoCompareIntegrationConfig import CryptoCompareConfig

markets_available = ('btc', 'eth', 'bch', 'ltc')
logger = logging.getLogger('FortacrypLogger')


def _validate_market_name(market_list: list):
    for market in market_list:
        if market not in markets_available:
            raise ValueError('market: {} is not recognized. Should be one of {}'.format(market, markets_available))


class KrakenConfig:
    def __init__(self, config):
        if not isinstance(config, CryptoCompareConfig):
            raise TypeError('Parameter config must be a CryptoCompareConfig instance')

        self.btc = None
        self.eth = None
        self.bch = None
        self.ltc = None

        markets = {
            'btc': {
                'subscription_pair': 'XBT/USD',
                'ohlc_pair': 'XBTUSD',
                'response_key': 'XXBTZUSD'
            },
            'eth': {
                'subscription_pair': 'ETH/USD',
                'ohlc_pair': 'ETHUSD',
                'response_key': 'XETHZUSD'
            },
            'bch': {
                'subscription_pair': 'BCH/USD',
                'ohlc_pair': 'BCHUSD',
                'response_key': 'BCHUSD'
            },
            'ltc': {
                'subscription_pair': 'LTC/USD',
                'ohlc_pair': 'LTCUSD',
                'response_key': 'XLTCZUSD'
            },
        }

        for market in markets:
            if hasattr(config, market):
                config_attr = getattr(config, market)
                attr = {
                    'completed': config_attr.recovered_all,
                    'last_timestamp': config_attr.last_stored_timestamp,
                    'subscription_pair': markets[market]['subscription_pair'],
                    'ohlc_pair': markets[market]['ohlc_pair'],
                    'response_key': markets[market]['response_key'],
                    'last_timestamp_socket': None,
                    'open': None,
                    'high': None,
                    'low': None,
                    'close': None,
                    'volume': 0
                }
                setattr(self, market, attr)


class KrakenHistoricalData:
    def __init__(self, market_list=('btc',)):
        _validate_market_name(market_list)
        self.base_path = './'
        self.csv_name = 'cryptoCompare_{}.csv'
        self.indexed_data = {}
        self.market_list = market_list

    def load_data(self):
        for market in self.market_list:
            path = os.path.join(self.base_path, self.csv_name.format(market))
            if not os.path.isfile(path):
                raise FileNotFoundError('Historical data file {} does not exist'.format(path))

            self.indexed_data[market] = pd.read_csv(path)

    def append(self, dict_data, market):
        ohlc = {
            'open': [dict_data['open'], ],
            'high': [dict_data['high'], ],
            'low': [dict_data['low'], ],
            'close': [dict_data['close'], ],
            'volumefrom': [dict_data['volume'], ]
        }
        if market not in self.indexed_data:
            raise KeyError('Market {} does not exists in indexed data')

        last_timestamp = dict_data['last_timestamp_socket']
        now = int(datetime.datetime.now().replace(minute=0, second=0, microsecond=0).timestamp())
        if abs(now - last_timestamp) > 3600:
            raise ValueError('Attempting to store a timestamp ({}) that is inconsistent with datetime.now()'
                             .format(last_timestamp))

        last_timestamp = datetime.datetime.fromtimestamp(last_timestamp).replace(minute=0, second=0, microsecond=0)
        last_timestamp = int(last_timestamp.timestamp())
        ohlc['time'] = [last_timestamp, ]
        new_row = pd.DataFrame(ohlc)

        dataframe = self.indexed_data[market]
        dataframe = dataframe.append(new_row, sort=False)
        # reorders the columns
        # https://stackoverflow.com/a/23741480
        # dataframe = dataframe[['time', 'open', 'high', 'low', 'close', 'volumefrom']]

        self.indexed_data[market] = dataframe
        self.indexed_data[market].to_csv(os.path.join(self.base_path, self.csv_name.format(market)))
        return dataframe


class KrakenIntegration:
    def __init__(self, config, market_list=('btc',)):
        self.ws = None
        self.requests = requests  # just to make it easier to test
        self.curr_close_timestamp = datetime.datetime.now() + datetime.timedelta(hours=1)
        self.curr_close_timestamp = self.curr_close_timestamp.replace(minute=0, second=0, microsecond=0)
        self.curr_close_timestamp = self.curr_close_timestamp.timestamp()

        if not isinstance(config, CryptoCompareConfig):
            raise TypeError('Parameter config must be a CryptoCompareConfig instance')

        _validate_market_name(market_list)
        self.config = KrakenConfig(config)
        self.market_list = {}

        for market in market_list:
            market_instance = getattr(self.config, market)
            if not market_instance['completed']:
                raise ValueError('market: {} has no historical data. Recover it first and the call this class.')

            self.market_list[market] = market_instance

    def subscribe(self):
        self._get_open_price()

        if self.ws is None:
            self._create_conection()

        def _subscribe(ws, on_ticket_callback):
            while True:
                try:
                    result = ws.recv()
                    result = json.loads(result)
                    if isinstance(result, list):
                        on_ticket_callback(result)

                except Exception as error:
                    logger.error(error)
                    raise ConnectionError('Exception in subscription to kraken websocket')

        threading.Thread(target=_subscribe, args=(self.ws, self._on_ticket)).run()

    def _on_ticket(self, ticket):
        last_trade = self._parse_ticket(ticket)

        print(last_trade)

    def _create_conection(self):
        pair = []
        for _, market in self.market_list.items():
            pair.append(market['subscription_pair'])

        logger.info('Connecting to kraken websocket...')
        for _ in range(3):
            try:
                self.ws = create_connection("wss://ws.kraken.com")
                logger.info('Subscribing to pairs {}'.format(pair))
                self.ws.send(json.dumps({
                    "event": "subscribe",
                    # "event": "ping",
                    # "pair": ["XBT/USD", ],
                    "pair": pair,
                    # "subscription": {"name": "ticker"}
                    # "subscription": {"name": "spread"}
                    "subscription": {"name": "trade"}
                    # "subscription": {"name": "book", "depth": 10}
                    # "subscription": {"name": "ohlc", "interval": 5}
                }))

            except Exception as error:
                print('Caught this error: ' + repr(error))
                self.ws = None
                time.sleep(3)
            else:
                break
        if self.ws is None:
            raise ConnectionError('Could not connect to kraken websocket')

    def _parse_ticket(self, socket_trade) -> dict:
        trade_list = socket_trade[constants.TRADE_LIST]
        trade = {
            'market': socket_trade[constants.MARKET_INDEX],
            'timestamp': float(trade_list[-1][constants.TIME_INDEX]),
            'price': float(trade_list[-1][constants.PRICE_INDEX])
        }

        volume = 0
        for entry in trade_list:
            volume += float(entry[constants.VOLUME_INDEX])

        trade['volume'] = volume
        return trade

    def _get_open_price(self):
        api_url = 'https://api.kraken.com/0/public/OHLC'

        for _, market in self.market_list.items():
            r = self.requests.get(api_url, {'pair': market['ohlc_pair'], 'interval': 60})
            if r.status_code != 200:
                raise ConnectionError('could not recover open price from kraken rest api for pair {}'
                                      .format(market['ohlc_pair']))

            json_response = json.loads(r.text)
            last_entry = json_response['result'][market['response_key']][-1]
            market['open'] = last_entry[constants.REST_OPEN_INDEX]
            market['high'] = last_entry[constants.REST_HIGH_INDEX]
            market['low'] = last_entry[constants.REST_LOW_INDEX]
            market['volume'] = last_entry[constants.REST_VOLUME_INDEX]

        return self.market_list

