import datetime
import json
import logging
import os
import threading
from typing import Optional

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


class KrakenSocketHandler(threading.Thread):
    """
    Class that manages the websocket to Kraken exchange,
    """

    def __init__(self, url='wss://ws.kraken.com', daemon_thread: bool = True):
        super().__init__(daemon=daemon_thread)
        self.socket_url: str = url
        self.ws = None
        self.reconnect_attempts_limit: int = 3
        self.reconnect_attempts: int = 0
        self.logger = logger
        self.pair: Optional[list] = None
        self.alertHandler = KrakenTelegramAlerts()
        self._kill_thread: bool = False
        self.on_new_price_callback: Optional[callable] = None

    def run(self) -> None:
        if self.pair is None or not isinstance(self.pair, list):
            raise AttributeError(
                'Pair attribute is not set. You should use one of connect function, instead of run directly')

        if self.on_new_price_callback is None:
            raise AttributeError('Callback function is not set.'
                                 ' You should use one of connect function, instead of run directly')

        _validate_market_name(self.pair)
        self._manage_thread()

    def connect_as_new_thread(self, pair: list, on_new_price_callback: callable):
        self._init_args(pair, on_new_price_callback)
        self.start()

    def connect_on_this_thread(self, pair: list, on_new_price_callback: callable):
        self._init_args(pair, on_new_price_callback)
        self.run()

    def kill_on_next_receiv(self):
        self._kill_thread = True

    def _init_args(self, pair: list, on_new_price_callback: callable):
        self.pair = pair
        self.on_new_price_callback = on_new_price_callback

    def _manage_thread(self):
        self.reconnect_attempts_limit = 1 if self.reconnect_attempts_limit <= 0 else self.reconnect_attempts_limit
        self.reconnect_attempts = 0

        while self.reconnect_attempts < self.reconnect_attempts_limit:
            if self.ws is None:
                self.reconnect_attempts += 1
                if self._create_connection():
                    self.reconnect_attempts = 0

            if self.ws is not None:
                exception = self._manage_connection()
                if exception is not None:
                    self.alertHandler.send_error_alert('Disconected from socket due to exception: {}'.format(exception))
                else:
                    break

        if not self._kill_thread:
            self.alertHandler.send_error_alert('Max Attempts to connect to socket exceeded')

    def _manage_connection(self):
        while True:
            try:
                if self._kill_thread:
                    return None

                result = self.ws.recv()
                result = json.loads(result)

                if isinstance(result, list):
                    self.on_new_price_callback(result)
            except Exception as e:
                self.ws.close()
                self.ws = None
                return e

    def _create_connection(self) -> bool:
        self.logger.info('Connecting to Kraken websocket')
        try:
            self.ws = create_connection(self.socket_url)
            self.logger.info('Subscribing to pairs {}'.format(self.pair))
            self.ws.send(json.dumps({
                "event": "subscribe",
                "pair": self.pair,
                "subscription": {"name": "trade"}
            }))
            return True

        except Exception as error:
            self.logger.error('Caught this error: ' + repr(error))
            if self.ws:
                self.ws.close()

            self.ws = None
            return False


class KrakenIntegration:
    def __init__(self, config, market_list=('btc',)):
        self.requests = requests  # just to make it easier to test by making easier to inject a mock
        self.curr_close_timestamp = datetime.datetime.now() + datetime.timedelta(hours=1)
        self.curr_close_timestamp = self.curr_close_timestamp.replace(minute=0, second=0, microsecond=0)
        self.curr_close_timestamp = self.curr_close_timestamp.timestamp()
        self.alert_sender = KrakenTelegramAlerts()
        self.websocket_handler = KrakenSocketHandler()
        self.logger = logger

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

        pair = []
        for _, market in self.market_list.items():
            pair.append(market['subscription_pair'])

        self.websocket_handler.connect(pair, self._on_ticket)
        self.websocket_handler.join()

    def _on_ticket(self, ticket):
        last_trade = self._parse_ticket(ticket)
        self.logger.info(last_trade)

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


class KrakenTelegramAlerts:
    def __init__(self):
        self.bot_id = os.getenv('FORTACRYP_BOT_ID', None)
        self.chat_id = os.getenv('FORTACRYP_CHAT_ID', None)
        self.should_send = self.bot_id is not None and self.chat_id is not None
        self.url = 'https://api.telegram.org/{}/sendMessage'.format(self.bot_id)
        self.requests = requests
        self.logger = logger

    def send_error_alert(self, message):
        if not self.should_send:
            return

        body = {
            'chat_id': self.chat_id,
            'text': 'Error: {}'.format(message)
        }
        r = self.requests.post(self.url, data=body)
        self._on_response(r)

    def _on_response(self, response):
        if response.status_code != 200:
            self.logger.info('Telegram alert sended successfully')
        else:
            self.logger.warning('Error sending Telegram Alert: {}'.format(response.text))
