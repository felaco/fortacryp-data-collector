import logging

from dateutil import tz
from datetime import datetime

from core.BaseIntegration import BaseCryptoIntegration
from core.config import MarketConfig
from cryptoCompare import CryptoCompareIntegrationConfig
from cryptoCompare.CryptoComparePersistence import CsvPersistor
import core.Constants as constants

logger = logging.getLogger('FortacrypLogger')


class CryptoCompareIntegration(BaseCryptoIntegration):
    def __init__(self, config: CryptoCompareIntegrationConfig):
        super().__init__(config, CsvPersistor('./'))
        self.to_currency = 'USD'

    def _generate_url(self, market_config: MarketConfig) -> str:
        if market_config.current_request_timestamp is not None:
            timestamp_url = '&toTs={}'.format(market_config.current_request_timestamp)
        else:
            timestamp_url = ''

        return '{}?limit=2000&fsym={}&tsym={}{}'.format(self.config.base_url,
                                                        market_config.market_id.upper(),
                                                        self.to_currency,
                                                        timestamp_url)

    def _do_loging(self, action, market_config: MarketConfig, **kwargs):
        if market_config.current_request_timestamp is None and action == constants.REQUESTED:
            return

        market_id = market_config.market_id
        if action == constants.REQUESTED:
            timestamp_sec = int(market_config.current_request_timestamp)

            local_tz = tz.gettz('America/Santiago')
            dt = datetime.fromtimestamp(timestamp_sec, tz=local_tz)
            string = dt.strftime("%d/%m/%Y %H:%M")

            logger.info('{} entries recovered from crypto compare. Date : {}'.format(self.persistor.market,
                                                                                     string))

        elif action == constants.UPDATED:
            logger.info('CryptoCompare-{}: entries has been updated'.format(market_id))
        elif action == constants.RECOVERED:
            logger.info('CryptoCompare-{}: entries has been recovered'.format(market_id))
        elif action == constants.EXCEPTION:
            follow_up = kwargs.get('exception', None)
            follow_up = ' With response code: {}'.format(follow_up.args[0]) if follow_up is not None else None
            logger.warning('CryptoCompare-{}: Houston we have a problem. We have been blocked!!!!!{}'.format(
                market_id,
                follow_up)
            )

    def _iterate_not_recovered_ending_condition(self, resp_json: dict, market_config: MarketConfig) -> bool:
        return resp_json['TimeFrom'] < self.config.retrieve_from_onward

    def _update_market_config(self, resp_json: dict, market_config: MarketConfig):
        pass

    def _get_first_timestamp_from_response(self, resp_json: dict) -> int:
        return int(resp_json['TimeTo'])

    def _get_last_timestamp_from_response(self, resp_json: dict) -> int:
        return int(resp_json['TimeFrom'])

    def _persist_new_entries(self, resp_json: dict, market_config: MarketConfig):
        self.persistor.persist(resp_json['Data'])
