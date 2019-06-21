from unittest import TestCase

from Buda.BudaIntegrationConfig import BudaMarketConfig
from core.configCore import _config
from core.config import root_config_from_dict


def deep_clone_dict(d: dict):
    """
    only deep clone with safety a dict with strings and numbers as values.
    if it has custom objects, those are not deep cloned
    """
    copy = {}

    for key, val in d.items():
        if isinstance(val, dict):
            copy[key] = deep_clone_dict(val)
        else:
            copy[key] = val

    return copy


class TestConfig(TestCase):
    def setUp(self):
        self.base_config = deep_clone_dict(_config)

    def compare_config_with_dict(self, config, dictionary: dict):
        for attr, val in dictionary.items():
            try:
                config_val = getattr(config, attr, None)
            except AttributeError:
                self.fail('Config object has no attribute ' + attr)

            if isinstance(val, dict):
                self.compare_config_with_dict(config_val, val)
            else:
                self.assertEqual(val, config_val)

    def compare_dict_with_dict(self, d1: dict, d2: dict):
        for key, val in d1.items():
            if isinstance(key, dict):
                self.compare_config_with_dict(val, d2[key])
            else:
                self.assertEqual(val, d2[key])

    def test_init_base_empty_config(self):
        config_obj = root_config_from_dict(self.base_config)
        self.compare_config_with_dict(config_obj, self.base_config)

    def test_from_dict_child_config(self):
        self.base_config['buda']['btc']['last_stored_timestamp'] = 1
        self.base_config['buda']['btc']['most_recent_timestamp'] = 2
        self.base_config['buda']['btc']['first_stored_timestamp'] = 3
        self.base_config['buda']['btc']['current_request_timestamp'] = 4

        config_obj = BudaMarketConfig.from_dict(self.base_config['buda'])
        self.compare_config_with_dict(config_obj, self.base_config['buda'])

    def test_from_dict_raises_exception(self):
        del self.base_config['buda']['btc']
        self.assertRaises(TypeError, BudaMarketConfig.from_dict, self.base_config['buda'])

    def test_to_dict_child_config(self):
        self.base_config['buda']['btc']['last_stored_timestamp'] = 1
        self.base_config['buda']['btc']['most_recent_timestamp'] = 2
        self.base_config['buda']['btc']['first_stored_timestamp'] = 3
        self.base_config['buda']['btc']['current_request_timestamp'] = 4

        config_obj = BudaMarketConfig.from_dict(self.base_config['buda'])
        ret_dict = config_obj.to_dict()
        self.compare_dict_with_dict(ret_dict, self.base_config['buda'])

    def test_reference_to_root_config_from_child(self):
        config = root_config_from_dict(self.base_config)
        self.assertEqual(id(config), id(config.buda.root_config))
