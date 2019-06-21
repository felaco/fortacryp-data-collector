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
        'sleep_time_sec': 1,
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
    """
    Class to store subconfiguration for a determined cryptocurrency. It is a convenience class in order
    to access its members with dot notation instead of bracket notation.
    Ok the names may not be the best or they may be confusing, but i dont want to change them 😭
    """

    def __init__(self, market_id: str, most_recent_timestamp: int = None,
                 current_request_timestamp: int = None, first_stored_timestamp: int = None,
                 recovered_all: bool = False, last_stored_timestamp: int = None):
        """
        init the config
        :param market_id: the cryptocurrency name in short format e.g. 'btc'
        :param most_recent_timestamp: stores the oldest transaction of the response when we have
        recovered all transaction previously. Usefull when restarting an aborted update
        (i think that last part is not supported yet)
        :param current_request_timestamp: stores the oldest transaction of the last request. Useful to know
        when we are recovering
        :param first_stored_timestamp: oldest timestamp that we have recovered.
        :param recovered_all: boolean indicating if we have recovered all data previously
        :param last_stored_timestamp: most recent timestamp that has been stored from a successfully
        ended process
        """
        self.last_stored_timestamp: int = last_stored_timestamp
        self.most_recent_timestamp: int = most_recent_timestamp
        self.first_stored_timestamp: int = first_stored_timestamp
        self.current_request_timestamp: int = current_request_timestamp
        self.market_id: str = market_id
        self.recovered_all: bool = recovered_all

    def to_dict(self):
        return self.__dict__


class BaseConfig(ABC):
    """
    Abstract class that serve as a base for the specific configuration of each data source.
    """
    root_config = None  # reference to the master config, since this is suposed to be a subconfig,
    # useful for asking the master config to persits itself

    def persist(self) -> None:
        """
        ask the root config to persist itself
        :return: nothing
        """
        if self.root_config is not None:
            self.root_config.persist()

    @classmethod
    @abstractmethod
    def get_instanciator(cls):
        """
        Get an instance of the child config using an empty constructor
        :return: instance of a child config
        """
        pass

    @classmethod
    @abstractmethod
    def get_market_config_instance(cls, **kwargs):
        """
        Get an instance of the child config pasing the keyed arguments to the
        constructor
        :param kwargs: arguments for the child class constructor
        :return: instance of a child config
        """
        pass

    @classmethod
    def from_dict(cls, config_dict: dict):
        """
        Construct a config based on a dictionary
        :param config_dict:
        :return: a config instance
        """
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

    def to_dict(self) -> dict:
        """
        returns this instance as a dictionary
        :return: a dictionary representing this instance
        """
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
    """
    The father config. The config that rules them all.
    If you want another data source, make sure to modify the from_dict method and add your new
    subconfig as an attribute to this class
    """
    buda = None
    crypto_compare = None

    @classmethod
    def get_instanciator(cls):
        return RootConfig()

    @classmethod
    def get_market_config_instance(cls, **kwargs):
        pass

    @classmethod
    def _is_valid_dict(cls, config_dict: dict):
        return 'crypto_compare' in config_dict and 'buda' in config_dict

    def persist(self):
        """
        Persist the config to a json file
        This class should be the only one that persist the config to the dict, child subconfig should
        have a reference to this config and ask this class to persist.
        :return:
        """
        config_dict = self.to_dict()

        json_str = json.dumps(config_dict, indent=4)
        dir_path = os.path.dirname(os.path.realpath(__file__))  # obtains the dir name from the current file
        dir_path = os.path.dirname(dir_path)  # obtains the parent folder
        abs_path = os.path.join(dir_path, 'config.json')

        with open(abs_path, mode='w', encoding='UTF-8') as file:
            file.write(json_str)

    def __dir__(self):
        return ['buda', 'crypto_compare']
