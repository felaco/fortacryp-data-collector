import json
from abc import ABC, abstractmethod
import time

import requests
from core.config import MarketConfig
from core.Constants import *


class BaseCryptoIntegration(ABC):
    def __init__(self, config, persistor):
        self.config = config
        self.persistor = persistor

    def recover_btc(self, market_id='btc'):
        self._generic_recover(market_id, persistor_name='btc', property_name='btc')

    def recover_ltc(self, market_id='ltc'):
        self._generic_recover(market_id, persistor_name='ltc', property_name='ltc')

    def recover_eth(self, market_id='eth'):
        self._generic_recover(market_id, persistor_name='ltc', property_name='ltc')

    def recover_bch(self, market_id='bch'):
        self._generic_recover(market_id, persistor_name='ltc', property_name='ltc')

    def _generic_recover(self, market_id, persistor_name, property_name):
        if not hasattr(self.config, property_name):
            setattr(self.config, property_name, MarketConfig(market_id))

        self.persistor.set_market(persistor_name)
        action = self._recover(getattr(self.config, property_name))
        self._do_loging(action, getattr(self.config, property_name))

    def _recover(self, market_config: MarketConfig):
        self._validate_persistor()
    
        if not market_config.recovered_all:
            self._not_all_recovered_generic_iteration(market_config)
            return RECOVERED
        else:
            self._iterate_generic(market_config)
            return UPDATED

    def _iterate_generic(self, market_config: MarketConfig):
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

    def _not_all_recovered_generic_iteration(self, market_config: MarketConfig):
        while not market_config.recovered_all:
            self._do_not_all_recovered_iteration(market_config)

        market_config.recovered_all = True  # have you ever tried to ctrl+S more than once, just to be sure?
        market_config.current_request_timestamp = None
        self.config.persist()

    def _do_not_all_recovered_iteration(self, market_config: MarketConfig):
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

    def _do_request(self, market_config: MarketConfig):
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

    def _validate_persistor(self):
        if hasattr(self.persistor, 'set_market') and \
                callable(self.persistor.set_market) and \
                hasattr(self.persistor, 'persist') and \
                callable(self.persistor.persist):

            return True
        else:
            raise TypeError('Persistor instance does not have all necesary methods. Is it really a persistor isntance?')

    @abstractmethod
    def _generate_url(self, market_config: MarketConfig) -> str:
        pass

    @abstractmethod
    def _do_loging(self, action, market_config: MarketConfig, **kwargs):
        pass

    @abstractmethod
    def _iterate_not_recovered_ending_condition(self, resp_json: dict, market_config: MarketConfig) -> bool:
        pass

    @abstractmethod
    def _update_market_config(self, resp_json: dict, market_config: MarketConfig):
        pass

    @abstractmethod
    def _get_first_timestamp_from_response(self, resp_json: dict) -> int:
        pass

    @abstractmethod
    def _get_last_timestamp_from_response(self, resp_json: dict) -> int:
        pass

    @abstractmethod
    def _persist_new_entries(self, resp_json: dict, market_config: MarketConfig):
        pass
