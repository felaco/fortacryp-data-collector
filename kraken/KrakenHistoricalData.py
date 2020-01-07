from datetime import tzinfo, timezone
from typing import Optional, Dict, Union

from core.BaseIntegration import BaseIntegration
from kraken.KrakenPersistors import KrakenPersistor
import kraken.KrakenConstants as Constants
import datetime

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


class KrakenHistoricalData(BaseIntegration):
    def __init__(self, persistor: Optional[KrakenPersistor] = None):
        super().__init__(persistor)
        self.since: Optional[Union[str, float]] = None

    def parse_response_to_list(self, market_config, response):
        def mapper(entry: list) -> dict:
            return {
                'price': entry[Constants.REST_PRICE_INDEX],
                'volume': entry[Constants.REST_TRADE_VOLUME_INDEX],
                'direction': entry[Constants.REST_DIRECTION_INDEX],
                'timestamp': entry[Constants.REST_TRADE_TS_INDEX]
            }

        market_key = _kraken_mapper[market_config['key']]['response_key']
        return list(map(mapper, response['result'][market_key]))

    def is_ending_condition_achieved(self, market_config, response_list):
        limit_date = datetime.datetime.now(timezone(datetime.timedelta(hours=0), 'UTC'))
        limit_date.replace(minute=0, second=0, microsecond=0)
        limit_ts = limit_date.timestamp() * 10 ** 9  # nanoseconds

        return response_list[-1]['timestamp'] >= limit_ts

    def generate_url(self, market_config) -> str:
        pair = market_config['ohlc_pair']
        if self.since is not None:
            since = self.since
        else:
            since = self.persistor.get_since(market_config['key']) * 1000  # transforms it to nanoseconds

        return f'https://api.kraken.com/0/public/Trades?pair={pair}&since={since}'

    def update_curr_ts(self, market_config, response):
        self.since = response['result']['last']
        return self.since

    def get_config_for_market(self, market_key) -> Dict[str, str]:
        return _kraken_mapper[market_key]

    def do_logging(self, market_config, log_type, message: Optional[str] = None, exception: Optional[Exception] = None):
        pass
