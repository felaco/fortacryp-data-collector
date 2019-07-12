from unittest import TestCase, mock
import numpy as np
import json
import urllib.parse as urlparse

from Buda.BudaIntegration import BudaIntegration
from Buda.BudaIntegrationConfig import BudaMarketTradeList, BudaMarketConfig
from core.configCore import MarketConfig


def get_entries_list():
    return [
        [1552966701233, 1, 0.1, 'buy'],  # 19/03/2019 03:38:21 | vol: 1
        [1552967141233, 2, 0.2, 'buy'],  # 19/03/2019 03:45:41 | vol: 3
        [1552967341233, 3, 0.3, 'buy'],  # 19/03/2019 03:49:01 | vol: 6
        [1552970041233, 4, 0.4, 'buy'],  # 19/03/2019 04:34:01 | vol: 4
        [1552972341233, 5, 0.5, 'buy'],  # 19/03/2019 05:12:21 | vol: 5
        [1552982341233, 6, 0.6, 'buy'],  # 19/03/2019 07:59:01 | vol: 6
        [1552986090421, 7, 0.7, 'buy'],  # 19/03/2019 09:01:30 | vol: 7
        [1552989090421, 8, 0.8, 'buy'],  # 19/03/2019 09:51:30 | vol: 15
        [1552989190421, 9, 0.9, 'buy'],  # 19/03/2019 09:53:10 | vol: 24
        [1552991190421, 9, 0.9, 'buy'],  # 19/03/2019 10:26:30 | vol: 9
        [1552999190421, 9, 0.9, 'buy'],  # 19/03/2019 12:39:50 | vol: 9
        [1553000000000, 9, 0.9, 'buy'],  # 19/03/2019 12:53:20 | vol: 18
        [1553002563546, 9, 0.9, 'buy'],  # 19/03/2019 13:36:03 | vol: 9
        [1553003758124, 9, 0.9, 'buy'],  # 19/03/2019 13:55:58 | vol: 18
        [1553003790421, 9, 0.9, 'buy'],  # 19/03/2019 13:56:30 | vol: 27
    ]


class BudaMarketTradeListTest(TestCase):
    def setUp(self):
        self.li = get_entries_list()
        self.volumes = [6, 4, 5, 0, 6, 0, 24, 9, 0, 18, 27]

    def test_calculate_volume_df(self):
        trade_list = BudaMarketTradeList()
        trade_list.append_raw(self.li)
        trade_list.resample_ohlcv()
        vol_series = np.asarray(self.volumes).astype(float)
        vol_pd = trade_list.trade_list['volume'].to_numpy()

        self.assertTrue(np.array_equal(vol_series, vol_pd))

    def test_merge(self):
        l1 = self.li[:7]  # split the list right at 9 am, so that timestamp is present in the 2 df when merged
        l2 = self.li[7:]
        tl1 = BudaMarketTradeList()
        tl1.append_and_resample(self.li)

        tl_copy1 = BudaMarketTradeList()
        tl_copy2 = BudaMarketTradeList()

        tl_copy1.append_raw(l1)
        tl_copy2.append_raw(l2)
        tl_copy1.resample_ohlcv()
        tl_copy2.resample_ohlcv()
        tl_copy1.merge(tl_copy2)

        self.assertTrue(tl1.trade_list.equals(tl_copy1.trade_list))

    def test_merge_fails_no_trade_list(self):
        l1 = self.li[:7]
        l2 = self.li[7:]
        tl1 = BudaMarketTradeList()
        tl1.append_and_resample(l1)

        self.assertRaises(TypeError, tl1.merge, l2)

    def test_merge_fails_cause_no_resampled_argument(self):
        l1 = self.li[:7]
        l2 = self.li[7:]

        tl1 = BudaMarketTradeList()
        tl1.append_and_resample(l1)
        tl2 = BudaMarketTradeList()
        tl2.append_raw(l2)

        self.assertRaises(ValueError, tl1.merge, tl2)

    def test_merge_self_not_resampled_should_work_too(self):
        l1 = self.li[:7]
        l2 = self.li[7:]
        tl1 = BudaMarketTradeList()
        tl1.append_and_resample(self.li)

        tl_copy1 = BudaMarketTradeList()
        tl_copy2 = BudaMarketTradeList()

        tl_copy1.append_raw(l1)
        tl_copy2.append_raw(l2)
        tl_copy2.resample_ohlcv()
        tl_copy1.merge(tl_copy2)

        self.assertTrue(tl1.trade_list.equals(tl_copy1.trade_list))


class MockResponse:
    def __init__(self, json_data: dict, status_code):
        self.text = json.dumps(json_data)
        self.status_code = status_code


m = mock.mock_open()


@mock.patch('time.sleep', return_value=True)
@mock.patch('builtins.open', m)
class BudaIntegrationTests(TestCase):

    def setUp(self):
        self.request_call_count = 0
        self.should_block = False
        self.market_config = MarketConfig('btc')
        self.buda_integration = BudaIntegration(BudaMarketConfig())
        self.buda_integration.should_log = False

    def test_executed_request_happy_case_not_recovered_all(self, *args):
        configuration = BudaMarketConfig()
        configuration.btc = self.market_config

        buda = BudaIntegration(configuration)
        # date utils gettz works as expected when used from cli, but fails when unittesting on
        # ubuntu (in windows works well) maybe it is related to mocking open() function (?)
        buda.should_log = False

        with mock.patch('core.BaseIntegration.requests.get', side_effect=self.mock_request_get):
            buda.recover_btc()

        self.assertGreaterEqual(self.request_call_count, 3)
        self.assertTrue(self.market_config.recovered_all)

    def test_wait_after_ddos_block(self, *args):
        configuration = BudaMarketConfig()
        configuration.btc = self.market_config

        buda = BudaIntegration(configuration)
        buda.should_log = False

        with mock.patch('core.BaseIntegration.requests.get', side_effect=self.mock_request_get):
            self.should_block = True
            buda.recover_btc()

        self.assertEqual(args[0].call_count, 3)
        self.assertEqual(self.request_call_count, 3)
        self.assertTrue(self.market_config.recovered_all)

    def test_ending_after_last_stored_is_less_than_config(self, *args):
        configuration = BudaMarketConfig()
        self.market_config.last_stored_timestamp = get_entries_list()[10][0]
        self.market_config.recovered_all = True

        configuration.btc = self.market_config
        buda = BudaIntegration(configuration)
        buda.should_log = False

        with mock.patch('core.BaseIntegration.requests.get', side_effect=self.mock_request_get):
            self.should_block = True
            buda.recover_btc()

        self.assertEqual(1, self.request_call_count)

    def test_generate_url(self, *args):
        burl = self.buda_integration._generate_url(self.market_config)
        url = 'https://www.buda.com/api/v2/markets/btc-clp/trades.json?limit=100'
        self.assertEqual(url, burl)

    def test_ending_condition_false(self, *args):
        response = {
            'trades': {
                'market_id': 'BTC-CLP',
                'timestamp': 123,
                'last_timestamp': 123,
                'entries': get_entries_list()
            }
        }
        r = self.buda_integration._iterate_not_recovered_ending_condition(response, self.market_config)
        self.assertFalse(r)

    def test_ending_condition_true(self, *args):
        response = {
            'trades': {
                'market_id': 'BTC-CLP',
                'timestamp': 123,
                'last_timestamp': None,
                'entries': []
            }
        }
        r = self.buda_integration._iterate_not_recovered_ending_condition(response, self.market_config)
        self.assertTrue(r)

    def mock_request_get(self, *args):
        if self.should_block:
            self.should_block = False
            return MockResponse({}, 501)

        url = args[0]
        # url example: https://www.buda.com/api/v2/markets/btc-clp/trades.json?limit=100&timestamp=1428430224012
        parsed = urlparse.urlparse(url)
        try:
            timestamp = int(urlparse.parse_qs(parsed.query)['timestamp'][0])
        except (AttributeError, KeyError):
            timestamp = None

        if timestamp is None:
            entries = get_entries_list()[:7]
            last_timestamp = entries[6][0]  # timestamp of the 7th entry
        else:
            entries = get_entries_list()[7:]
            last_timestamp = entries[-1][0]

        if self.request_call_count >= 2:
            last_timestamp = None

        self.request_call_count += 1
        response = {
            'trades': {
                'market_id': 'BTC-CLP',
                'timestamp': timestamp,
                'last_timestamp': last_timestamp,
                'entries': entries
            }
        }

        return MockResponse(response, 200)
