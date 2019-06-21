import datetime
import json
import logging
import os
import threading
from typing import Optional, Any, Dict, Union, List

import requests
import krakenWebSocket.KrakenConstants as Constants

import pandas as pd
from websocket import create_connection

from cryptoCompare.CryptoCompareIntegrationConfig import CryptoCompareConfig

_markets_available = ('btc', 'eth', 'bch', 'ltc')
logger = logging.getLogger('FortacrypLogger')


def _validate_market_name(market_list: List[str]):
    for market in market_list:
        if market not in _markets_available:
            raise ValueError('market: {} is not recognized. Should be one of {}'.format(market, _markets_available))


class KrakenConfig:
    def __init__(self, config: CryptoCompareConfig):
        if not isinstance(config, CryptoCompareConfig):
            raise TypeError('Parameter config must be a CryptoCompareConfig instance')

        self.btc: Optional[Dict[str, Any]] = None
        self.eth: Optional[Dict[str, Any]] = None
        self.bch: Optional[Dict[str, Any]] = None
        self.ltc: Optional[Dict[str, Any]] = None

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
    def __init__(self, market_list: List[str] = ('btc',)):
        _validate_market_name(market_list)
        self.base_path: str = './'
        self.csv_name: str = 'cryptoCompare_{}.csv'
        self.indexed_data: Dict[str, pd.DataFrame] = {}
        self.market_list: List[str] = market_list

    def load_data(self) -> None:
        for market in self.market_list:
            path = os.path.join(self.base_path, self.csv_name.format(market))
            if not os.path.isfile(path):
                raise FileNotFoundError('Historical data file {} does not exist'.format(path))

            self.indexed_data[market] = pd.read_csv(path)

    def append(self, dict_data: Dict[str, Union[float, int]], market: str) -> pd.DataFrame:
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
    Class that manages the websocket to Kraken exchange, has some reconection abilities
    when something goes wrong. Since extends from Thread can be used in a new thread or in the same one
    depending of the way you call it.

    socket = KrakenSocketHandler()
    socket.connect_as_new_thread(['btc], callback)

    manages the socket in a new daemon thread and calls the callback when a new price arrives.
    Note that you have to use a new instance when a thread stop after being created.

    socket.connect_as_new_thread(['btc], callback)
    socket.join()
    socket.connect_as_new_thread(['btc], callback)

    is an illegal way of using a Thread, so it will throw a runtime error.

    socket.connect_on_this_thread()  # is a blocking operation, since it runs in the same thread
    but can be stopped safely and be reused.

    To stop a socket you can do it with:
    socket.kill_on_next_receiv()

    it will stop the socket, whether be a new thread or not AFTER a new price arrives. This happens
    because there is no way of forcefully kill a thread in python, so it will just check a condition in
    a while loop and this check ocurrs after the function socket.reveiv() stop of blocking the thread.
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

    def connect_as_new_thread(self, pair: list, on_new_price_callback: callable) -> None:
        self._init_args(pair, on_new_price_callback)
        self.start()

    def connect_on_this_thread(self, pair: list, on_new_price_callback: callable) -> None:
        self._kill_thread = False
        self._init_args(pair, on_new_price_callback)
        self.run()

    def kill_on_next_receiv(self) -> None:
        self._kill_thread = True

    def _init_args(self, pair: list, on_new_price_callback: callable) -> None:
        self.pair = pair
        self.on_new_price_callback = on_new_price_callback

    def _manage_thread(self) -> None:
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

    def _manage_connection(self) -> Optional[Exception]:
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
        self.curr_close_timestamp: datetime.datetime = datetime.datetime.now() + datetime.timedelta(hours=1)
        self.curr_close_timestamp: datetime.datetime = self.curr_close_timestamp.replace(minute=0, second=0,
                                                                                         microsecond=0)
        self.curr_close_timestamp: float = self.curr_close_timestamp.timestamp()
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

    def subscribe(self) -> None:
        self._get_open_price()

        pair = []
        for _, market in self.market_list.items():
            pair.append(market['subscription_pair'])

        self.websocket_handler.connect_on_this_thread(pair, self._on_ticket)
        self.websocket_handler.join()

    def _on_ticket(self, ticket: list) -> None:
        last_trade = self._parse_ticket(ticket)
        self.logger.info(last_trade)

    def _parse_ticket(self, socket_trade: list) -> dict:
        trade_list = socket_trade[Constants.TRADE_LIST]
        trade = {
            'market': socket_trade[Constants.MARKET_INDEX],
            'timestamp': float(trade_list[-1][Constants.TIME_INDEX]),
            'price': float(trade_list[-1][Constants.PRICE_INDEX])
        }

        volume = 0
        for entry in trade_list:
            volume += float(entry[Constants.VOLUME_INDEX])

        trade['volume'] = volume
        return trade

    def _get_open_price(self) -> Dict[str, Any]:
        api_url = 'https://api.kraken.com/0/public/OHLC'

        for _, market in self.market_list.items():
            r = self.requests.get(api_url, {'pair': market['ohlc_pair'], 'interval': 60})
            if r.status_code != 200:
                raise ConnectionError('could not recover open price from kraken rest api for pair {}'
                                      .format(market['ohlc_pair']))

            json_response = json.loads(r.text)
            last_entry = json_response['result'][market['response_key']][-1]
            market['open'] = last_entry[Constants.REST_OPEN_INDEX]
            market['high'] = last_entry[Constants.REST_HIGH_INDEX]
            market['low'] = last_entry[Constants.REST_LOW_INDEX]
            market['volume'] = last_entry[Constants.REST_VOLUME_INDEX]

        return self.market_list


class KrakenTelegramAlerts:
    def __init__(self):
        self.bot_id = os.getenv('FORTACRYP_BOT_ID', None)
        self.chat_id = os.getenv('FORTACRYP_CHAT_ID', None)
        self.should_send = self.bot_id is not None and self.chat_id is not None
        self.url = 'https://api.telegram.org/{}/sendMessage'.format(self.bot_id)
        self.requests = requests
        self.logger = logger

    def send_error_alert(self, message) -> None:
        if not self.should_send:
            return

        body = {
            'chat_id': self.chat_id,
            'text': 'Error: {}'.format(message)
        }
        r = self.requests.post(self.url, data=body)
        self._on_response(r)

    def _on_response(self, response) -> None:
        if response.status_code != 200:
            self.logger.info('Telegram alert sended successfully')
        else:
            self.logger.warning('Error sending Telegram Alert: {}'.format(response.text))
