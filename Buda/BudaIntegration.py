import json
import requests
import time
import logging

from Buda.BudaIntegrationConfig import BudaMarketConfig, MarketsId, MarketConfig, BudaMarketTradeList, buda_config_from_file
from Buda.BudaPersistence import BudaPersistenceBase, BudaCsvPersistence

logger = logging.getLogger('BudaLogger')
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s:%(name)s:%(message)s")


class BudaIntegration:
    def __init__(self, config: BudaMarketConfig = None, config_path: str = None,
                 buda_persistor: BudaPersistenceBase = None):

        if config is not None:
            if isinstance(config, BudaMarketConfig):
                self.config = config
            else:
                raise AssertionError('config must be a BudaMarketConfig instance.')
        else:
            self.config = buda_config_from_file(config_path)

        self.config_path = config_path

        if buda_persistor is not None:
            assert issubclass(buda_persistor.__class__, BudaPersistenceBase)
            self.persistor = buda_persistor
        else:
            self.persistor = BudaCsvPersistence('./')

    def recover_btc(self):
        if self.config.btc is None:
            self.config.btc = MarketConfig(MarketsId.btc)

        self.persistor.set_market('btc')

        if not self.config.btc.recovered_all:
            # if not all entries has been retrieved, then continues chaining requests until
            # the server responds no more entries
            while not self.config.btc.recovered_all:
                self._do_recover_iteration_not_recover_all(self.config.btc)

            self.config.btc.recovered_all = True
            self.config.btc.current_request_timestamp = None
            self.config.persist()

            logger.info('BTC: All entries has been recovered.')

        else:

            store_last_timestamp = True

            # recovers data until current request timestamp reaches last stored timestamp
            # stores the last timestamp from the first attempt in most recent timestamp
            while self.config.btc.current_request_timestamp is None or \
                    self.config.btc.current_request_timestamp > self.config.btc.last_stored_timestamp:

                try:
                    resp_json = self._recover(self.config.btc)

                    if store_last_timestamp:
                        self.config.btc.most_recent_timestamp = int(resp_json['trades']['entries'][0][0])
                        store_last_timestamp = False

                    self.config.persist()

                    self._do_logging(int(self.config.btc.current_request_timestamp))
                    time.sleep(self.config.sleep_time_sec)

                except(requests.RequestException, ConnectionError):
                    logger.warning('Houston we have a problem. We have been blocked!!!!!')
                    time.sleep(self.config.sleep_time_after_block)

            logger.info('BTC: entries has been sucessfully updated.')
            self.config.btc.last_stored_timestamp = self.config.btc.most_recent_timestamp
            self.config.btc.current_request_timestamp = None
            self.config.persist()

    def _do_recover_iteration_not_recover_all(self, market_config: MarketConfig):
        try:
            resp_json = self._recover(market_config)

            if 'entries' not in resp_json['trades'] or \
                    len(resp_json['trades']['entries']) == 0 or \
                    resp_json['trades']['last_timestamp'] is None:
                market_config.recovered_all = True

            if market_config.last_stored_timestamp is None:
                # rescue the timestamp column of the first entry. timestamp is index 0
                market_config.last_stored_timestamp = int(resp_json['trades']['entries'][0][0])

            if market_config.current_request_timestamp is not None:
                market_config.first_stored_timestamp = int(market_config.current_request_timestamp)

            self.config.persist()
            self._do_logging(market_config.current_request_timestamp)

            time.sleep(self.config.sleep_time_sec)

        except (requests.RequestException, ConnectionError):
            logger.warning('Houston we have a problem. We have been blocked!!!!!')
            time.sleep(self.config.sleep_time_after_block)

    def _recover(self, market_config: MarketConfig):
        if market_config.current_request_timestamp:
            timestamp_str = '&timestamp={}'.format(market_config.current_request_timestamp)
        else:
            timestamp_str = ''

        url = 'https://www.buda.com/api/v2/markets/{}/trades.json?limit=100{}'.format(market_config.market_id,
                                                                                      timestamp_str)

        r = requests.get(url)
        if r.status_code != 200:
            logger.warning('Response with code: {}'.format(r.status_code))
            raise ConnectionError

        resp_json = json.loads(r.text)
        buda_list = BudaMarketTradeList()
        buda_list.append_raw(resp_json['trades']['entries'])

        self.persistor.persist(buda_list)

        market_config.current_request_timestamp = int(resp_json['trades']['last_timestamp'])
        return resp_json

    def _do_logging(self, timestamp_ms):
        if timestamp_ms is None:
            return

        from dateutil import tz
        from datetime import datetime

        timestamp_sec = float(timestamp_ms) / 1000.0

        local_tz = tz.gettz('America/Santiago')
        dt = datetime.fromtimestamp(timestamp_sec, tz=local_tz)
        string = dt.strftime("%d/%m/%Y %H:%M")

        logger.info('{} entries recovered. Date : {}'.format(self.persistor.market, string))


if __name__ == '__main__':
    integration = BudaIntegration()
    integration.recover_btc()
