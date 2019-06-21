import json
from unittest import TestCase, mock
import pandas as pd
import numpy as np
import datetime
import logging

from core.configCore import _config
from core.config import root_config_from_dict
from krakenWebSocket.KrakenIntegration import KrakenIntegration, KrakenHistoricalData, KrakenSocketHandler

df = pd.DataFrame(data=np.arange(12).reshape(2, 6),
                  columns=['time', 'open', 'high', 'low', 'close', 'volumefrom'])

m = mock.mock_open()
logger = logging.getLogger('dummy')
logger.addHandler(logging.NullHandler())


@mock.patch('builtins.open', m)
class KrakenHistoricalDataTests(TestCase):
    def setUp(self) -> None:
        self.new_data = {
            'open': 100,
            'high': 101,
            'low': 99,
            'close': 100.5,
            'volume': 411,
            'last_timestamp_socket': datetime.datetime.now().timestamp() - 1800
        }
        self.df = df.copy(deep=True)

    def test_init_fail(self):
        with self.assertRaises(ValueError):
            KrakenHistoricalData(['btc', 'not_existing_market'])

    def test_merge_success_and_col_order_unchanged(self):
        kraken = KrakenHistoricalData()
        kraken.indexed_data['btc'] = self.df
        appended = kraken.append(self.new_data, 'btc')

        colname1 = np.asarray(self.df.columns.values)
        colname2 = np.asarray(appended.columns.values)
        # https://stackoverflow.com/a/10580782
        self.assertTrue((colname1 == colname2).all())

        last_datetime = datetime.datetime.fromtimestamp(self.new_data['last_timestamp_socket'])
        last_datetime = last_datetime.replace(minute=0, second=0, microsecond=0).timestamp()
        last_row = appended.tail(1)

        self.assertEqual(last_row['time'][0], int(last_datetime))
        self.assertEqual(float(last_row['close'][0]), float(self.new_data['close']))
        self.assertEqual(float(last_row['open'][0]), float(self.new_data['open']))
        self.assertEqual(float(last_row['high'][0]), float(self.new_data['high']))
        self.assertEqual(float(last_row['low'][0]), float(self.new_data['low']))
        self.assertEqual(float(last_row['volumefrom'][0]), float(self.new_data['volume']))

    def test_merge_fail_timestamp_diff(self):
        kraken = KrakenHistoricalData()
        kraken.indexed_data['btc'] = self.df

        timestamp = int(datetime.datetime.now().replace(second=0, minute=0, microsecond=0).timestamp())
        self.new_data['last_timestamp_socket'] = timestamp - 3600
        # should not raise an exception
        kraken.append(self.new_data, 'btc')

        kraken = KrakenHistoricalData()
        kraken.indexed_data['btc'] = self.df

        self.new_data['last_timestamp_socket'] = timestamp - 3601
        with self.assertRaises(ValueError):
            kraken.append(self.new_data, 'btc')

    def test_merge_fail_no_market(self):
        kraken = KrakenHistoricalData()
        kraken.indexed_data['btc'] = self.df
        with self.assertRaises(KeyError):
            kraken.append(self.new_data, 'non_existing_market')


class DummyWebScocket:
    def __init__(self):
        self.initial_timestamp = 1534614057.321597
        self.exception_after_n_responses = None
        self.call_count = 0

    def close(self):
        pass

    def send(self, *args, **kwargs):
        pass

    def recv(self):
        self.call_count += 1
        if self.exception_after_n_responses is not None and self.call_count > self.exception_after_n_responses:
            raise ConnectionError('Dummy Exception')

        self.initial_timestamp += 1
        return json.dumps([
            0,
            [
                [
                    "5541.20000",
                    "0.15850568",
                    str(self.initial_timestamp),
                    "s",
                    "l",
                    ""
                ],
                [
                    "6060.00000",
                    "0.02455000",
                    str(self.initial_timestamp + 0.1),
                    "b",
                    "l",
                    ""
                ]
            ],
            "trade",
            "XBT/USD"
        ])


class DummyResponse:
    def __init__(self):
        self.status_code = 200
        self.text = '[]'


class DummyRequests:
    def __init__(self):
        self.initial_timestamp = 1534614057.321597

    def get(self, *args, **kwargs):
        response = DummyResponse()
        response.text = json.dumps({
            'result': {
                'XXBTZUSD': [
                    [
                        self.initial_timestamp,
                        "9181.0",
                        "9181.0",
                        "9106.1",
                        "9150.7",
                        "9137.3",
                        "127.50098346",
                        481
                    ],
                ],
                'XETHZUSD': [
                    [
                        self.initial_timestamp,
                        "9181.0",
                        "9181.0",
                        "9106.1",
                        "9150.7",
                        "9137.3",
                        "127.50098346",
                        481
                    ],
                ],
                'XLTCZUSD': [
                    [
                        self.initial_timestamp,
                        "9181.0",
                        "9181.0",
                        "9106.1",
                        "9150.7",
                        "9137.3",
                        "127.50098346",
                        481
                    ],
                ],
            }
        })
        return response


@mock.patch('builtins.open', m)
class KrakenIntegrationTest(TestCase):
    def setUp(self) -> None:
        root_config = root_config_from_dict(_config)
        root_config.crypto_compare.btc.recovered_all = True
        self.kraken = KrakenIntegration(root_config.crypto_compare)

    def test_parse_ticket(self):
        dummy = DummyWebScocket()
        dummy.initial_timestamp = 122
        dummy = json.loads(dummy.recv())

        expected = {
            'market': 'XBT/USD',
            'timestamp': 123.1,
            'price': 6060.0,
            'volume': 0.18305568
        }

        parsed = self.kraken._parse_ticket(dummy)
        self.assertEqual(parsed, expected)

    def test_fail_if_no_init(self):
        root_config = root_config_from_dict(_config)
        with self.assertRaises(ValueError):
            KrakenIntegration(root_config.crypto_compare)

    def test_get_open_price(self):
        self.kraken.requests = DummyRequests()
        expected = {
            'open': '9181.0',
            'high': '9181.0',
            'low': '9106.1',
            'volume': '127.50098346',
            'close': None
        }

        market = self.kraken._get_open_price()['btc']
        result = {'open': market['open'], 'high': market['high'], 'low': market['low'],
                  'volume': market['volume'], 'close': market['close']}

        self.assertEqual(expected, result)
        config = root_config_from_dict(_config).crypto_compare
        config.btc.recovered_all = True
        config.eth.recovered_all = True
        config.ltc.recovered_all = True

        kraken = KrakenIntegration(config, ['btc', 'eth', 'ltc'])
        kraken.requests = DummyRequests()
        market_list = kraken._get_open_price()

        self.assertEqual(3, len(market_list.keys()))
        for _, market in market_list.items():
            result = {'open': market['open'], 'high': market['high'], 'low': market['low'],
                      'volume': market['volume'], 'close': market['close']}
            self.assertEqual(result, expected)


class KrakenAlertDummy:
    def __init__(self):
        self.error_call_count = 0
        self.last_message: str = ''

    def send_error_alert(self, msg):
        self.last_message = msg
        self.error_call_count += 1


class KrakenSocketHandlerTests(TestCase):
    def setUp(self) -> None:
        self.handler = KrakenSocketHandler()
        self.handler.ws = DummyWebScocket()
        self.handler.alertHandler = KrakenAlertDummy()
        self.callback_count = 0
        self.exception_after_n_calls = 0
        self.exception_on_create_ws = False
        self.should_exception_on_create_ws = False
        self.handler.logger = logger

    def test_fails_if_call_run_directly(self):
        with self.assertRaises(AttributeError):
            self.handler.run()

        self.handler.pair = ['btc', 'eth']
        with self.assertRaises(AttributeError):
            self.handler.run()

        self.handler.on_new_price_callback = self._dummy_callback
        self.handler.pair = ['bad market']
        markets_available = ('btc', 'eth', 'bch', 'ltc')

        try:
            self.handler.run()
            self.fail('asd')
        except Exception as e:
            self.assertTrue(isinstance(e, ValueError))
            msg = 'market: {} is not recognized. Should be one of {}'.format('bad market', markets_available)
            self.assertEqual(e.args[0], msg)

    def test_exit_on_kill_switch(self):
        self.handler.connect_as_new_thread(['btc'], self._dummy_callback)

        callback_count = self.callback_count
        self.handler.kill_on_next_receiv()
        self.handler.join()

        self.assertEqual(self.callback_count, callback_count + 1)
        # well if this test case fails, should hang forever (?)
        self.assertFalse(self.handler.is_alive())

    def test_reconnect_attemps(self):
        with mock.patch('krakenWebSocket.KrakenIntegration.create_connection',
                        side_effect=self._create_connection_dummy_mock):

            self.should_exception_on_create_ws = True
            self.exception_after_n_calls = 2
            self.handler.ws = None
            self.handler.connect_on_this_thread(['btc'], self._dummy_callback)
            self.assertEqual(self.callback_count, 2)
            self.assertEqual(self.handler.reconnect_attempts, 3)

    def _create_connection_dummy_mock(self, url):
        if self.exception_on_create_ws:
            raise ConnectionError('Dummy connection error')

        if self.should_exception_on_create_ws:
            self.exception_on_create_ws = True

        ws = DummyWebScocket()
        if self.exception_after_n_calls > 0:
            ws.exception_after_n_responses = self.exception_after_n_calls

        return ws

    def _dummy_callback(self, ticket):
        self.callback_count += 1
