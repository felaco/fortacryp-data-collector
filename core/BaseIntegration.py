import datetime
import json
from abc import ABC, abstractmethod
import time
from enum import Enum
from typing import Union, Optional

import requests
from core.configCore import MarketConfig
from core.Constants import *


class IntegrationMarkets(Enum):
    BTC = 'btc'
    ETH = 'eth'
    BCH = 'bch'
    LTC = 'ltc'


class BaseCryptoIntegration(ABC):
    """
    Base class to recover historical cryptocurrency data from a server. Usually a server doesnt respond
    with all the historical data at once. So a certain logic must be made to chain request one after
    another in order to obtain all data. This class use a config file in order to store what timestamp
    are we recovering now and if we have already recovered all historical data and now we are just updating
    with new data. This is done like this because the ending condition is diferent for each case, and the way
    to update the config file is different too.
    """

    def __init__(self, config, persistor):
        """
        init the class with the configuration
        :param config: BaseIntegration instance
        :param persistor: instance that is used to save the new data in a not volatile way.
        The only requeriment for a persistor is that it implements a set_market and persist methods
        """
        self.config = config
        self.persistor = persistor

    def recover_btc(self, market_id='btc') -> None:
        self._generic_recover(market_id, persistor_name='btc', property_name='btc')

    def recover_ltc(self, market_id='ltc') -> None:
        self._generic_recover(market_id, persistor_name='ltc', property_name='ltc')

    def recover_eth(self, market_id='eth') -> None:
        self._generic_recover(market_id, persistor_name='ltc', property_name='ltc')

    def recover_bch(self, market_id='bch') -> None:
        self._generic_recover(market_id, persistor_name='ltc', property_name='ltc')

    def _generic_recover(self, market_id, persistor_name, property_name) -> None:
        """
        Configure the persistor to store data for the cryptocurrency required and
        execute calls the iteration method to execute the chained requests
        :param market_id: name of the cryptocurrency in short format, usually btc, ltc, eth or bch
        :param persistor_name: a name that is used as a way for the persistor recognize wich data is storing
        :param property_name: the name of the attribute of the config file corresponding to the required
        crypto currency. For example if in the config there is a subconfiguration stored as a 'btc' key,
        property_name should be 'btc'
        :return: nothing
        """
        if not hasattr(self.config, property_name):
            setattr(self.config, property_name, MarketConfig(market_id))

        self.persistor.set_market(persistor_name)
        action = self._recover(getattr(self.config, property_name))
        self._do_loging(action, getattr(self.config, property_name))

    def _recover(self, market_config: MarketConfig) -> str:
        """
        Decides if wich way we should iterate, if we have already recovered all data before or not.
        In each case there is a different way of iterate
        :param market_config: subconfiguration for a certain cryptocurrency
        :return: a constant indicating which action has been made
        """
        self._validate_persistor()

        if not market_config.recovered_all:
            self._not_all_recovered_generic_iteration(market_config)
            return RECOVERED
        else:
            self._iterate_generic(market_config)
            return UPDATED

    def _iterate_generic(self, market_config: MarketConfig) -> None:
        """
        Way of iterating when we have already recovered all data before, so we want to update new data
        instead.
        :param market_config: subconfiguration for a certain cryptocurrency
        :return:
        """
        store_last_timestamp = True

        # recovers data until current request timestamp reaches last stored timestamp
        # stores the last timestamp from the first attempt in most recent timestamp
        while market_config.current_request_timestamp is None \
                or market_config.current_request_timestamp > market_config.last_stored_timestamp:

            try:
                resp_json = self._do_request(market_config)

                if store_last_timestamp:
                    store_last_timestamp = False
                    market_config.most_recent_timestamp = self._get_first_timestamp_from_response(resp_json)

                self.config.persist()
                self._do_loging(REQUESTED, market_config)

                if hasattr(self.config, 'sleep_time_sec'):
                    time.sleep(self.config.sleep_time_sec)

            except (requests.RequestException, ConnectionError):
                self._do_loging(EXCEPTION, market_config)
                if hasattr(self.config, 'sleep_time_after_exception'):
                    time.sleep(self.config.sleep_time_after_exception)

        market_config.last_stored_timestamp = market_config.most_recent_timestamp
        market_config.current_request_timestamp = None
        self.config.persist()

    def _not_all_recovered_generic_iteration(self, market_config: MarketConfig) -> None:
        """
        Execute the iteration for the case we have not recovered all historical.
        :param market_config: subconfiguration for a certain cryptocurrency
        :return:
        """
        while not market_config.recovered_all:
            self._do_not_all_recovered_iteration(market_config)

        market_config.most_recent_timestamp = market_config.last_stored_timestamp
        market_config.recovered_all = True  # have you ever tried to ctrl+S more than once, just to be sure?
        market_config.current_request_timestamp = None
        self.config.persist()

    def _do_not_all_recovered_iteration(self, market_config: MarketConfig) -> None:
        """
        Inner function for the iteration when the historical data has not been recovered before.
        Calls the function that makes the request to the server, updates the config file and persist it
        to the disk. Has some logic to wait a little in between calls to prevent the server think we are
        making a ddos attack. Has a little logic to wait even more time in case of error comunicating with
        the server, mainly in case the server block us either way.
        :param market_config: subconfiguration for a certain cryptocurrency
        :return:
        """
        try:
            resp_json = self._do_request(market_config)
            self._update_market_config(resp_json, market_config)

            if self._iterate_not_recovered_ending_condition(resp_json, market_config):
                market_config.recovered_all = True

            if market_config.last_stored_timestamp is None:
                market_config.last_stored_timestamp = self._get_first_timestamp_from_response(resp_json)

            if market_config.current_request_timestamp is not None:
                market_config.first_stored_timestamp = int(market_config.current_request_timestamp)

            self.config.persist()
            self._do_loging(REQUESTED, market_config)

            if hasattr(self.config, 'sleep_time_sec'):
                time.sleep(self.config.sleep_time_sec)

        except (requests.RequestException, ConnectionError) as e:
            self._do_loging(EXCEPTION, market_config, exception=e)
            if hasattr(self.config, 'sleep_time_after_exception'):
                time.sleep(self.config.sleep_time_after_exception)

    def _do_request(self, market_config: MarketConfig) -> dict:
        """
        Execute the request call to the server, calls the function to persist the response data,
        updates the config to standarize the way we know wich timestamp we have got, so we can
        send it to server to chain requests. Raises an error in case the server does not respond
        with status 200 OK.
        :param market_config: subconfiguration for a certain cryptocurrency
        :return: the response json transformed into a python dictionary
        """
        url = self._generate_url(market_config)
        r = requests.get(url)

        if r.status_code != 200:
            raise ConnectionError(r.status_code)

        resp_json = json.loads(r.text)
        self._persist_new_entries(resp_json, market_config)

        current_request_timestamp = self._get_last_timestamp_from_response(resp_json)
        if current_request_timestamp is not None:
            market_config.current_request_timestamp = current_request_timestamp
        else:
            market_config.current_request_timestamp = None

        return resp_json

    def _validate_persistor(self) -> bool:
        if hasattr(self.persistor, 'set_market') and \
                callable(self.persistor.set_market) and \
                hasattr(self.persistor, 'persist') and \
                callable(self.persistor.persist):

            return True
        else:
            raise TypeError('Persistor instance does not have all necesary methods. Is it really a persistor isntance?')

    @abstractmethod
    def _generate_url(self, market_config: MarketConfig) -> str:
        """
        Abstract method. Should return the string url to make the request, that includes the get params
        encoded in it
        :param market_config: subconfiguration for a certain cryptocurrency
        :return: the string url
        """
        pass

    @abstractmethod
    def _do_loging(self, action, market_config: MarketConfig, **kwargs) -> None:
        """
        Executes the loging  based on the action passed
        :param action: action indicating what is the thing we are trying to log
        :param market_config: subconfiguration for a certain cryptocurrency
        :param kwargs: additional arguments. The only place that is used is passing the
         exception with 'exception' key
        :return: nothing
        """
        pass

    @abstractmethod
    def _iterate_not_recovered_ending_condition(self, resp_json: dict, market_config: MarketConfig) -> bool:
        """
        Checks the end condition for the iteration when we have not recovered all data yet.
        :param resp_json: the json response of the server. It is a dict
        :param market_config: subconfiguration for a certain cryptocurrency
        :return: a boolean
        """
        pass

    @abstractmethod
    def _update_market_config(self, resp_json: dict, market_config: MarketConfig) -> None:
        """
        Not used. Not sure why i added it. Too afraid to delete
        :param resp_json: the json response of the server. It is a dict
        :param market_config: subconfiguration for a certain cryptocurrency
        :return: nothing
        """
        pass

    @abstractmethod
    def _get_first_timestamp_from_response(self, resp_json: dict) -> int:
        """
        Abstract method. Should return the most recent timestamp of the response.
        :param resp_json: the json response of the server. It is a dict
        :return: most recent timestamp of the response
        """
        pass

    @abstractmethod
    def _get_last_timestamp_from_response(self, resp_json: dict) -> Union[int, None]:
        """
        Abstract method. Should return the oldest timestamp of the response
        :param resp_json: the json response of the server. It is a dict
        :return: oldest timestamp of the response
        """
        pass

    @abstractmethod
    def _persist_new_entries(self, resp_json: dict, market_config: MarketConfig) -> None:
        """
        Calls the persistor in order to save new data
        :param resp_json: the json response of the server. It is a dict
        :param market_config: subconfiguration for a certain cryptocurrency
        :return: nothing
        """
        pass


def _timestamp_to_str(timestamp: int) -> str:
    date = datetime.datetime.fromtimestamp(timestamp, datetime.timezone.utc)
    return date.strftime('%Y-%m-%d %H:%M')


class CoreIntegration(ABC):
    def __init__(self, configuration=None, persistor=None):
        self.config = configuration
        self.persistor = persistor
        self.requests = requests

    def recover(self, market: IntegrationMarkets):
        if not hasattr(self.config, market.value):
            self.do_logging(CRITICAL, None, f'Configuration has no attribute {market.value}')
        else:
            self.do_main_loop(getattr(self.config, market.value))

    def do_main_loop(self, market_config):
        last_data = None
        while not self.is_ending_condition_achieved(last_data):
            try:
                response_list = self.do_request(market_config)
                self.persistor.persist(response_list)
                last_data = self.update_last_data(response_list)
                from_date = _timestamp_to_str(self.get_older_entry_ts(response_list))
                to_date = _timestamp_to_str(self.get_most_recent_entry_ts(response_list))
                self.do_logging(UPDATED, market_config, f'Recovered data from {from_date} to {to_date} GMT-0')

            except (requests.RequestException, ConnectionError) as e:
                self.do_logging(EXCEPTION, market_config, str(e))
                if hasattr(self.config, 'sleep_time_after_exception'):
                    time.sleep(self.config.sleep_time_after_exception)

        self.do_logging(RECOVERED, market_config)

    def do_request(self, market_config):
        r = self.requests.get(self.generate_url(market_config))
        if r.status_code != 200:
            raise ConnectionError(f'Response code: {r.status_code} from server')

        return self.parse_response_to_list(json.loads(r.text), market_config)

    @abstractmethod
    def generate_url(self, market_config) -> str:
        raise NotImplementedError()

    @abstractmethod
    def is_ending_condition_achieved(self, last_data: Optional[dict]) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def update_last_data(self, data_list) -> int:
        raise NotImplementedError()

    @abstractmethod
    def parse_response_to_list(self, response, market_config=None) -> list:
        raise NotImplementedError()

    @abstractmethod
    def do_logging(self, action: str, market_config, msg: Optional[str] = None) -> None:
        raise NotImplementedError()


class ForwardRecoverIntegration(CoreIntegration, ABC):
    def update_last_data(self, data_list) -> int:
        return self.get_most_recent_entry_ts(data_list)

    @abstractmethod
    def get_most_recent_entry_ts(self, data_list) -> int:
        pass
