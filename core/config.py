import os
import json
from abc import ABC, abstractmethod
import logging

from core.utils import is_valid_market_json

logger = logging.getLogger('BudaLogger')
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s:%(name)s:%(message)s")

_config = {
    'crypto_compare': {
        'baseUrl': 'https://min-api.cryptocompare.com/data/histohour',
        'api_key': None,  # replace with your api key
        #  timestamps should be in seconds. Thats what crypto compare responds
        'retrieve_from_onward': 1364774400,  # last timestamp from where we should recover data
        # 2013 / 04 / 01 : 00:00:00
        'sleep_time_after_block': 300,
        'btc': {
            'last_stored_timestamp': None,
            'most_recent_timestamp': None,
            'first_stored_timestamp': None,
            'current_request_timestamp': None,
            'recovered_all': False,
            'market_id': 'btc'
        },
        'ltc': {
            'last_stored_timestamp': None,
            'most_recent_timestamp': None,
            'first_stored_timestamp': None,
            'current_request_timestamp': None,
            'recovered_all': False,
            'market_id': 'ltc'
        },
        'eth': {
            'last_stored_timestamp': None,
            'most_recent_timestamp': None,
            'first_stored_timestamp': None,
            'current_request_timestamp': None,
            'recovered_all': False,
            'market_id': 'eth'
        },
        'bch': {
            'last_stored_timestamp': None,
            'most_recent_timestamp': None,
            'first_stored_timestamp': None,
            'current_request_timestamp': None,
            'recovered_all': False,
            'market_id': 'bch'
        }
    },
    'buda': {
        'base-url': 'https://www.buda.com/api/v2/markets/',
        'sleep_time_sec': 10,
        'sleep_time_after_block': 300,
        'resample_interval': '1H',
        #  timestamps should be in milliseconds. Thats what crypto compare responds
        'btc': {
            'last_stored_timestamp': None,
            'most_recent_timestamp': None,
            'first_stored_timestamp': None,
            'current_request_timestamp': None,
            'recovered_all': False,
            'market_id': 'btc'
        },
        'bch': {
            'last_stored_timestamp': None,
            'most_recent_timestamp': None,
            'first_stored_timestamp': None,
            'current_request_timestamp': None,
            'recovered_all': False,
            'market_id': 'bch'
        },
        'eth': {
            'last_stored_timestamp': None,
            'most_recent_timestamp': None,
            'first_stored_timestamp': None,
            'current_request_timestamp': None,
            'recovered_all': False,
            'market_id': 'eth'
        },
        'ltc': {
            'last_stored_timestamp': None,
            'most_recent_timestamp': None,
            'first_stored_timestamp': None,
            'current_request_timestamp': None,
            'recovered_all': False,
            'market_id': 'ltc'
        }
    }
}


class MarketConfig:

    def __init__(self, market_id: str, most_recent_timestamp: int = None,
                 current_request_timestamp: int = None, first_stored_timestamp: int = None,
                 recovered_all: bool = False, last_stored_timestamp: int = None):
        self.last_stored_timestamp = last_stored_timestamp
        self.most_recent_timestamp = most_recent_timestamp
        self.first_stored_timestamp = first_stored_timestamp
        self.current_request_timestamp = current_request_timestamp
        self.market_id = market_id
        self.recovered_all: bool = recovered_all

    def to_dict(self):
        return self.__dict__


class BaseConfig(ABC):
    root_config = None

    def persist(self):
        if self.root_config is not None:
            self.root_config.persist()

    @classmethod
    @abstractmethod
    def get_instanciator(cls):
        pass

    @classmethod
    @abstractmethod
    def get_market_config_instance(cls, **kwargs):
        pass

    @classmethod
    def from_dict(cls, config_dict: dict):
        if is_valid_market_json(config_dict):
            buda_config = cls.get_instanciator()

            for key in config_dict.keys():
                val = config_dict[key]

                if isinstance(val, dict):
                    setattr(buda_config, key, cls.get_market_config_instance(**val))
                else:
                    setattr(buda_config, key, val)

            return buda_config
        else:
            raise TypeError('Configuration cannot be created from config dict')

    def to_dict(self):
        config_dict = {}
        attrs = dir(self)

        for attr in attrs:
            if attr == 'root_config':
                pass
            obj = getattr(self, attr)
            if callable(getattr(obj, 'to_dict', None)):
                config_dict[attr] = obj.to_dict()
            elif isinstance(obj, (int, str)) or obj is None:
                config_dict[attr] = obj
            else:
                raise TypeError('Cannot serialize attribute: ' + attr)

        return config_dict

    def get_market_config(self, market: str):
        return getattr(self, market)


class RootConfig(BaseConfig):

    @classmethod
    def get_instanciator(cls):
        return RootConfig()

    @classmethod
    def get_market_config_instance(cls, **kwargs):
        pass

    @classmethod
    def from_dict(cls, config_dict: dict):
        from Buda.BudaIntegrationConfig import BudaMarketConfig
        from cryptoCompare.CryptoCompareIntegrationConfig import CryptoCompareConfig

        if cls._is_valid_dict(config_dict):
            root_instance = cls.get_instanciator()

            buda = BudaMarketConfig.from_dict(config_dict['buda'])
            buda.root_config = root_instance

            crypto_compare = CryptoCompareConfig.from_dict(config_dict['crypto_compare'])
            crypto_compare.root_config = root_instance

            root_instance.buda = buda
            root_instance.crypto_compare = crypto_compare

            return root_instance
        else:
            raise TypeError('Cannot reconstruct Root config from config file')

    @classmethod
    def _is_valid_dict(cls, config_dict: dict):
        return 'crypto_compare' in config_dict and 'buda' in config_dict

    def persist(self):
        config_dict = self.to_dict()

        json_str = json.dumps(config_dict, indent=4)
        dir_path = os.path.dirname(os.path.realpath(__file__))  # obtains the dir name from the current file
        dir_path = os.path.dirname(dir_path)  # obtains the parent folder
        abs_path = os.path.join(dir_path, 'config.json')

        with open(abs_path, mode='w', encoding='UTF-8') as file:
            file.write(json_str)

    def __dir__(self):
        return ['buda', 'crypto_compare']


def load_config(persist_on_create=False):
    dir_path = os.path.dirname(os.path.realpath(__file__))  # current project path.
    dir_path = os.path.dirname(dir_path)  # parent folder
    # useful for executing this script using cron and storing the config in the project folder,
    # not the user folder
    abs_path = os.path.join(dir_path, 'config.json')
    if os.path.isfile(abs_path):
        try:
            with open(abs_path) as file:
                r = file.read()
                json_dict = json.loads(r)
                return RootConfig.from_dict(json_dict)

        except (TypeError, json.JSONDecodeError):
            logger.warning('An error ocurred while deserializing the config file.'
                           ' Using the default one instead')
            root_config = RootConfig.from_dict(_config)
            root_config.persist()
            return root_config

    else:
        root_config = RootConfig.from_dict(_config)
        if persist_on_create:
            root_config.persist()
        return root_config


config = load_config()
