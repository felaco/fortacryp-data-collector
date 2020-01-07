import datetime
import json
import logging
import threading
from typing import Optional, Any, Dict, Union, Tuple, List

import requests
from websocket import create_connection

import kraken.KrakenConstants as Constants
from core.BaseIntegration import BaseCryptoIntegration
from core.configCore import MarketConfig
from cryptoCompare.CryptoCompareIntegrationConfig import CryptoCompareConfig
from kraken.KrakenAlerts import KrakenTelegramAlerts, KrakenBaseAlerts
from kraken.KrakenPersistors import KrakenPersistor
from kraken.KrakenTicketHandler import BaseKrakenTicketHandler

_markets_available = ('btc', 'eth', 'bch', 'ltc')
logger = logging.getLogger('FortacrypLogger')


def _validate_market_name(market: str):
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
                'response_key': 'XXBTZUSD',
                'key': 'btc'
            },
            'eth': {
                'subscription_pair': 'ETH/USD',
                'ohlc_pair': 'ETHUSD',
                'response_key': 'XETHZUSD',
                'key': 'eth'
            },
            'bch': {
                'subscription_pair': 'BCH/USD',
                'ohlc_pair': 'BCHUSD',
                'response_key': 'BCHUSD',
                'key': 'bch'
            },
            'ltc': {
                'subscription_pair': 'LTC/USD',
                'ohlc_pair': 'LTCUSD',
                'response_key': 'XLTCZUSD',
                'key': 'ltc'
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
                    'key': markets[market]['key']
                }
                setattr(self, market, attr)


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

        # for market in self.pair:
        #     _validate_market_name(market)
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
                exception, string = self._manage_connection()
                if exception is not None:
                    self.alertHandler.send_error_alert('Disconected from socket due to exception: {} '
                                                       '.Response: {}'.format(exception, string))
                else:
                    break

        if not self._kill_thread:
            self.alertHandler.send_error_alert('Max Attempts to connect to socket exceeded')

    def _manage_connection(self) -> Optional[Tuple[Exception, str]]:
        while True:
            response = None
            try:
                if self._kill_thread:
                    return None

                result = self.ws.recv()
                response = result
                result = json.loads(result)

                if isinstance(result, list):
                    self.on_new_price_callback(result)
            except Exception as e:
                self.ws.close()
                self.ws = None
                return e, response

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


class TicketHandler:
    def __init__(self):
        self.alert_handler = None


def _ticket_list_to_dict(socket_trade: list) -> Dict[str, Union[float, str]]:
    mapper = {
        'XBT/USD': 'btc',
        'ETH/USD': 'eth',
        'BCH/USD': 'bch',
        'LTC/USD': 'ltc'
    }

    trade_list = socket_trade[Constants.TRADE_LIST]
    trade = {
        'market': mapper[socket_trade[Constants.MARKET_INDEX]],
        'timestamp': float(trade_list[-1][Constants.TIME_INDEX]),
        'price': float(trade_list[-1][Constants.PRICE_INDEX])
    }

    volume = 0
    for entry in trade_list:
        volume += float(entry[Constants.VOLUME_INDEX])

    trade['volume'] = volume
    return trade


_kraken_mapper = markets = {
    'btc': {
        'subscription_pair': 'XBT/USD',
        'ohlc_pair': 'XBTUSD',
        'response_key': 'XXBTZUSD',
        'key': 'btc'
    },
    'eth': {
        'subscription_pair': 'ETH/USD',
        'ohlc_pair': 'ETHUSD',
        'response_key': 'XETHZUSD',
        'key': 'eth'
    },
    'bch': {
        'subscription_pair': 'BCH/USD',
        'ohlc_pair': 'BCHUSD',
        'response_key': 'BCHUSD',
        'key': 'bch'
    },
    'ltc': {
        'subscription_pair': 'LTC/USD',
        'ohlc_pair': 'LTCUSD',
        'response_key': 'XLTCZUSD',
        'key': 'ltc'
    },
}


class KrakenIntegration:
    def __init__(self, config, market_list=('btc',)):
        self.requests = requests  # just to make it easier to test by making easier to inject a mock
        # stores the timestamp on which a new hourly candle will be generated
        self.curr_close_timestamp: datetime.datetime = datetime.datetime.now() + datetime.timedelta(hours=1)
        self.curr_close_timestamp: datetime.datetime = self.curr_close_timestamp.replace(minute=0, second=0,
                                                                                         microsecond=0)
        self.curr_close_timestamp: float = self.curr_close_timestamp.timestamp()
        self.alert_sender: KrakenBaseAlerts = KrakenTelegramAlerts()
        self.websocket_handler = KrakenSocketHandler()
        self.ticket_handler: BaseKrakenTicketHandler = BaseKrakenTicketHandler()
        self.logger = logger

        if not isinstance(config, CryptoCompareConfig):
            raise TypeError('Parameter config must be a CryptoCompareConfig instance')

        self.config = KrakenConfig(config)
        self.market_list: Dict[str, dict] = {}

        for market in market_list:
            _validate_market_name(market)

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
        last_trade = _ticket_list_to_dict(ticket)

        self.ticket_handler.on_new_ticket(last_trade)
        self.logger.info(last_trade)

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

            self.ticket_handler.init_open_data(market['key'], market['open'], market['high'],
                                               market['low'], market['volume'])

        return self.market_list


class KrakenHistoricalDataIntegration(BaseCryptoIntegration):
    def __init__(self, persistor: KrakenPersistor = None, pair: str = 'XBTUSD'):
        if persistor is None:
            persistor = KrakenPersistor()

        super().__init__(None, persistor)
        self.pair = pair
        self._market_response_key = 'XXBTZUSD'
        self.last_timestamp = persistor.last_timestamp()

    def _generic_recover(self, market_id: str, persistor_name, property_name) -> None:
        limit_timestamp = datetime.datetime.now().replace(minute=0, second=0, microsecond=0).timestamp()

        while self.last_timestamp < limit_timestamp:
            r = self._do_request(None)
            self.last_timestamp = r[-1]['timestamp']
            self.persistor.persist(r)

    def _do_request(self, market_config: Optional[MarketConfig]) -> List[Dict[str, Union[float, int]]]:
        url = self._generate_url(None)
        response = requests.get(url)

        if response != 200:
            raise ConnectionError(response.status_code)

        response = json.loads(response.text)
        return self._parse_response_to_list(response)

    def _generate_url(self, market_config: Optional[MarketConfig]) -> str:
        path = 'https://api.kraken.com/0/public/OHLC?pair={}&interval=60&since={}'
        return path.format(self.pair, self.last_timestamp)

    def _do_loging(self, action, market_config: MarketConfig, **kwargs) -> None:
        pass

    def _iterate_not_recovered_ending_condition(self, resp_json: dict, market_config: MarketConfig) -> bool:
        pass

    def _update_market_config(self, resp_json: dict, market_config: MarketConfig) -> None:
        pass

    def _get_first_timestamp_from_response(self, resp_json: dict) -> int:
        pass

    def _get_last_timestamp_from_response(self, resp_json: dict) -> Union[int, None]:
        pass

    def _persist_new_entries(self, resp_json: dict, market_config: MarketConfig) -> None:
        pass

    def _parse_response_to_list(self, response: dict):
        err = response['error']
        if len(err) > 0:
            raise ConnectionAbortedError(err)

        li = response['result'][self._market_response_key]
        parsed_list: List[Dict[str, Union[float, int]]] = []

        for entry in li:
            parsed = {
                'timestamp': entry[Constants.REST_TIMESTAMP_INDEX],
                'open': entry[Constants.REST_OPEN_INDEX],
                'high': entry[Constants.REST_HIGH_INDEX],
                'low': entry[Constants.REST_LOW_INDEX],
                'close': entry[Constants.REST_CLOSE_INDEX],
                'volume': entry[Constants.REST_VOLUME_INDEX]
            }
            parsed_list.append(parsed)

        return parsed_list
