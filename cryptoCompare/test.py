from unittest import TestCase
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
