import json
from unittest import TestCase, mock

from core.config import MarketConfig
from cryptoCompare.CryptoCompareIntegration import CryptoCompareIntegration
from cryptoCompare.CryptoCompareIntegrationConfig import CryptoCompareConfig
from cryptoCompare.CryptoComparePersistence import _merge_prepend, _merge_append, _tick_to_line, \
    _merge_stored_with_recovered_lists


class PersistenseTests(TestCase):

    def setUp(self) -> None:
        self.stored = [
            'time,open,high,low,close,volumefrom',
            '1554109200,4138.59,4142.9,4134.55,4141.95,1074.9',
            '1554112800,4137.59,4141.9,4133.55,4140.95,1073.9',
            '1554116400,4136.59,4140.9,4132.55,4139.95,1072.9',
            '1554120000,4135.59,4139.9,4131.55,4138.95,1071.9',
            '1554123600,4134.59,4138.9,4130.55,4137.95,1070.9',
            '1554127200,4133.59,4137.9,4129.55,4136.95,1069.9',
        ]
        self.tickets_prepend = [
            {"time": 1554094800, "close": 4138.26, "high": 4143.31, "low": 4134.65, "open": 4143.27,
             "volumefrom": 1028.36, "volumeto": 4250514.88},
            {"time": 1554098400, "close": 4143.78, "high": 4143.97, "low": 4137.7, "open": 4138.26,
             "volumefrom": 1504.6, "volumeto": 6219680.27},
            {"time": 1554102000, "close": 4139.73, "high": 4146.69, "low": 4136.14, "open": 4143.78,
             "volumefrom": 1728.73, "volumeto": 7178526.43},
            {"time": 1554105600, "close": 4141.95, "high": 4142.37, "low": 4128.21, "open": 4139.73,
             "volumefrom": 1451.68, "volumeto": 5989942.75},
            {"time": 1554109200, "close": 4138.59, "high": 4142.9, "low": 4134.55, "open": 4141.95,
             "volumefrom": 1074.9, "volumeto": 4457508.96}
        ]

        self.tickets_append = [
            {"time": 1554120000, "close": 4143.38, "high": 4144.42, "low": 4130.48, "open": 4130.6,
             "volumefrom": 1265.87, "volumeto": 5244181.83},
            {"time": 1554123600, "close": 4150.08, "high": 4150.37, "low": 4140.91, "open": 4143.38,
             "volumefrom": 1691.04, "volumeto": 7006157.01},
            {"time": 1554127200, "close": 4150.18, "high": 4164.33, "low": 4146.56, "open": 4150.08,
             "volumefrom": 3167.56, "volumeto": 13174235.9},
            {"time": 1554130800, "close": 4152.03, "high": 4155.92, "low": 4146.22, "open": 4150.18,
             "volumefrom": 1596.32, "volumeto": 6626727.27},
            {"time": 1554134400, "close": 4157.7, "high": 4161.11, "low": 4152.03, "open": 4152.03,
             "volumefrom": 1318.01, "volumeto": 5468993.19},
            {"time": 1554138000, "close": 4147.92, "high": 4157.81, "low": 4146.65, "open": 4157.7,
             "volumefrom": 1219.62, "volumeto": 5058297.6},
        ]

    def test_tick_to_csv_line(self):
        tickets = [
            {"time": 1554094800, "close": 4138.26, "high": 4143.31, "low": 4134.65, "open": 4143.27,
             "volumefrom": 1028.36, "volumeto": 4250514.88},
            {"time": 1554098400, "close": 4143.78, "high": 4143.97, "low": 4137.7, "open": 4138.26,
             "volumefrom": 1504.6, "volumeto": 6219680.27}
        ]
        result = _tick_to_line(tickets)
        self.assertEqual(2, len(result))
        self.assertEqual('1554094800,4143.27,4143.31,4134.65,4138.26,1028.36', result[0])
        self.assertEqual('1554098400,4138.26,4143.97,4137.7,4143.78,1504.6', result[1])

    def test_merge_prepend(self):
        result = _merge_prepend(self.tickets_prepend, self.stored)

        # self.stored is length 7; tickets is length 5, but timestamp 1554109200 should only be stored
        # once
        self.assertEqual(11, len(result))

    def test_merge_prepend_empty_tickets(self):
        tickets = []
        result = _merge_prepend(tickets, self.stored)
        self.assertEqual(7, len(result))

    def test_merge_prepend_null_tickets(self):
        tickets = None
        result = _merge_prepend(tickets, self.stored)
        self.assertEqual(7, len(result))

    def test_merge_append(self):
        result = _merge_append(self.tickets_append, self.stored)
        self.assertEqual(10, len(result))

    def test_merge_append_empty_tickets(self):
        tickets = []
        result = _merge_append(tickets, self.stored)
        self.assertEqual(7, len(result))

    def test_merge_append_null_tickets(self):
        tickets = None
        result = _merge_append(tickets, self.stored)
        self.assertEqual(7, len(result))

    def test_merge_with_recover_list_prepend(self):
        # Assumes _merge_prepend functions works correctly. Pretty big assumption
        copy_stored = self.stored.copy()

        result_prepend = _merge_prepend(self.tickets_prepend, copy_stored)
        result = _merge_stored_with_recovered_lists(self.tickets_prepend, self.stored)
        self.assertEqual(result_prepend, result)

    def test_merge_with_recover_list_append(self):
        # Assumes _merge_append functions works correctly. Pretty big assumption
        copy_tickets = self.tickets_append.copy()

        result_append = _merge_append(self.tickets_append, self.stored)
        result = _merge_stored_with_recovered_lists(copy_tickets, self.stored)
        self.assertEqual(result_append, result)

    def test_merge_with_recover_list_empty_ticks(self):
        ticks = []
        copy_stored = self.stored.copy()

        result = _merge_stored_with_recovered_lists(ticks, self.stored)
        self.assertEqual(copy_stored, result)


class MockResponse:
    def __init__(self, json_data: dict, status_code):
        self.text = json.dumps(json_data)
        self.status_code = status_code


m = mock.mock_open()


@mock.patch('time.sleep', return_value=True)
@mock.patch('builtins.open', m)
class CryptoCompareIntegrationTest(TestCase):
    def setUp(self) -> None:
        config = CryptoCompareConfig()
        config.retrieve_from_onward = 1364774400
        self.integration = CryptoCompareIntegration(config)
        self.call_count = 0
        self.market_config = MarketConfig('btc')

        self.response_1 = {
            "Response": "Success",
            "Type": 100,
            "Aggregated": False,
            "Data": [
                {
                    "time": 1554796800,
                    "close": 5233.31,
                    "high": 5244.9,
                    "low": 5188.55,
                    "open": 5228.83,
                    "volumefrom": 3238.31,
                    "volumeto": 16907516.72
                },
                {
                    "time": 1554800400,
                    "close": 5197.37,
                    "high": 5241.76,
                    "low": 5184.56,
                    "open": 5233.31,
                    "volumefrom": 2674.25,
                    "volumeto": 13931609.37
                },
                {
                    "time": 1554804000,
                    "close": 5208.93,
                    "high": 5215.43,
                    "low": 5160.98,
                    "open": 5197.37,
                    "volumefrom": 3155.77,
                    "volumeto": 16351469.7
                },
                {
                    "time": 1554807600,
                    "close": 5215.99,
                    "high": 5224.7,
                    "low": 5185.98,
                    "open": 5208.93,
                    "volumefrom": 2677.37,
                    "volumeto": 13949648.16
                },
                {
                    "time": 1554811200,
                    "close": 5199.54,
                    "high": 5227.47,
                    "low": 5198.26,
                    "open": 5215.99,
                    "volumefrom": 1574.24,
                    "volumeto": 8194954.25
                },
                {
                    "time": 1554814800,
                    "close": 5234.45,
                    "high": 5248.03,
                    "low": 5197.39,
                    "open": 5199.54,
                    "volumefrom": 1141.91,
                    "volumeto": 5964250.08
                }
            ],
            "TimeTo": 1554814800,
            "TimeFrom": 1554796800,
            "FirstValueInArray": True,
            "ConversionType": {
                "type": "direct",
                "conversionSymbol": ""
            },
            "RateLimit": {},
            "HasWarning": False
        }

        self.response_2 = {
            "Response": "Success",
            "Type": 100,
            "Aggregated": False,
            "Data": [
                {
                    "time": 1554778800,
                    "close": 5241.62,
                    "high": 5274.48,
                    "low": 5222.14,
                    "open": 5259.71,
                    "volumefrom": 2808.05,
                    "volumeto": 14748945
                },
                {
                    "time": 1554782400,
                    "close": 5230.45,
                    "high": 5257.67,
                    "low": 5216.86,
                    "open": 5241.62,
                    "volumefrom": 2174.88,
                    "volumeto": 11387159
                },
                {
                    "time": 1554786000,
                    "close": 5219.86,
                    "high": 5233.86,
                    "low": 5159.13,
                    "open": 5230.45,
                    "volumefrom": 3423.08,
                    "volumeto": 17794135.37
                },
                {
                    "time": 1554789600,
                    "close": 5239.14,
                    "high": 5240.69,
                    "low": 5215.18,
                    "open": 5219.86,
                    "volumefrom": 1404.51,
                    "volumeto": 7346936.97
                },
                {
                    "time": 1554793200,
                    "close": 5228.83,
                    "high": 5246.31,
                    "low": 5219.2,
                    "open": 5239.14,
                    "volumefrom": 1287.1,
                    "volumeto": 6728131.28
                },
                {
                    "time": 1554796800,
                    "close": 5233.31,
                    "high": 5244.9,
                    "low": 5188.55,
                    "open": 5228.83,
                    "volumefrom": 3238.31,
                    "volumeto": 16907516.72
                }
            ],
            "TimeTo": 1554796800,
            "TimeFrom": 1554778800,
            "FirstValueInArray": True,
            "ConversionType": {
                "type": "direct",
                "conversionSymbol": ""
            },
            "RateLimit": {},
            "HasWarning": False
        }
        #  response from long ago. Used to test if the chaining of request should end
        self.response_3 = {
            "Response": "Success",
            "Type": 100,
            "Aggregated": False,
            "Data": [
                {
                    "time": 1364770800,
                    "close": 100,
                    "high": 100,
                    "low": 100,
                    "open": 100,
                    "volumefrom": 100,
                    "volumeto": 100
                },
                {
                    "time": 1364774400,
                    "close": 100,
                    "high": 100,
                    "low": 100,
                    "open": 100,
                    "volumefrom": 100,
                    "volumeto": 100
                },
                {
                    "time": 1364778000,
                    "close": 100,
                    "high": 100,
                    "low": 100,
                    "open": 100,
                    "volumefrom": 100,
                    "volumeto": 100
                }
            ],
            "TimeTo": 1364778000,
            "TimeFrom": 1364770800,
            "FirstValueInArray": True,
            "ConversionType": {
                "type": "direct",
                "conversionSymbol": ""
            },
            "RateLimit": {},
            "HasWarning": False
        }

    def test_generate_url_no_timestamp(self, *args):
        url = self.integration._generate_url(self.market_config)
        expected = 'https://min-api.cryptocompare.com/data/histohour?limit=2000&fsym=BTC&tsym=USD'
        self.assertEqual(expected, url)

    def test_generate_url_with_timestamp(self, *args):
        self.market_config.current_request_timestamp = 123456
        expected = 'https://min-api.cryptocompare.com/data/histohour?limit=2000&fsym=BTC&tsym=USD&toTs=123456'
        url = self.integration._generate_url(self.market_config)
        self.assertEqual(expected, url)

    def test_end_condition_function_true(self, *args):
        r = self.integration._iterate_not_recovered_ending_condition(self.response_3, self.market_config)
        self.assertTrue(r)

    def test_end_condition_function_false(self, *args):
        r = self.integration._iterate_not_recovered_ending_condition(self.response_2, self.market_config)
        self.assertFalse(r)

    def test_get_last_timestamp_from_request(self, *args):
        timestamp = self.integration._get_last_timestamp_from_response(self.response_1)
        self.assertEqual(1554796800, timestamp)

    def test_end_condition_after_recovered_all(self, *args):
        self.market_config.recovered_all = True
        # a timestamp that ends the process after only 1 request call
        self.market_config.last_stored_timestamp = 1554807600
        self.integration.config.btc = self.market_config

        with mock.patch('core.BaseIntegration.requests.get', side_effect=self.mock_request_get):
            self.integration.recover_btc()

        self.assertEqual(1, self.call_count)

    def test_recover_all_data_from_start_condition(self, *args):
        """
        test the process to recover all data. From having an empty config to have all data
        persisted. Since the requests are mocked, it should only execute 3 calls, but the integration
        has to recognize the end condition.
        """
        with mock.patch('core.BaseIntegration.requests.get', side_effect=self.mock_request_get):
            self.integration.recover_btc()

        self.assertEqual(3, self.call_count)

    def mock_request_get(self, *args):
        # if call_count is 0 or timestamp is none
        response = self.response_1

        self.call_count += 1
        if self.call_count == 2:
            response = self.response_2

        if self.call_count > 2:
            response = self.response_3
        return MockResponse(response, 200)
