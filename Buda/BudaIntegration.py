import logging
from typing import Union

from Buda.BudaIntegrationConfig import BudaMarketConfig, MarketsId, BudaMarketTradeList
from Buda.BudaPersistence import BudaCsvPersistence

from core.BaseIntegration import BaseCryptoIntegration
from core.configCore import MarketConfig
import core.Constants as constants

from dateutil import tz
from datetime import datetime

logger = logging.getLogger('FortacrypLogger')


class BudaIntegration(BaseCryptoIntegration):
    def __init__(self, config: BudaMarketConfig):
        super().__init__(config, BudaCsvPersistence('./'))
        self.should_log = True

    def _generate_url(self, market_config: MarketConfig) -> str:
        market_id = getattr(MarketsId, market_config.market_id)
        if market_config.current_request_timestamp is not None:
            timestamp_str = '&timestamp={}'.format(market_config.current_request_timestamp)
        else:
            timestamp_str = ''

        return '{}{}/trades.json?limit=100{}'.format(
            self.config.base_url,
            market_id,
            timestamp_str
        )

    def _do_loging(self, action, market_config: MarketConfig, **kwargs):
        if not self.should_log:
            return

        if market_config.current_request_timestamp is None and action == constants.REQUESTED:
            return

        market_id = market_config.market_id
        if action == constants.REQUESTED:
            timestamp_sec = float(market_config.current_request_timestamp) / 1000.0

            local_tz = tz.gettz('America/Santiago')
            dt = datetime.fromtimestamp(timestamp_sec, tz=local_tz)
            string = dt.strftime("%d/%m/%Y %H:%M")

            logger.info('{} entries recovered. Date : {}'.format(self.persistor.market, string))

        elif action == constants.UPDATED:
            logger.info('BUDA-{}: entries has been updated'.format(market_id))
        elif action == constants.RECOVERED:
            logger.info('BUDA-{}: entries has been recovered'.format(market_id))
        elif action == constants.EXCEPTION:
            follow_up = kwargs.get('exception', None)
            follow_up = ' With response code: {}'.format(follow_up.args[0]) if follow_up is not None else None
            logger.warning('BUDA-{}: Houston we have a problem. We have been blocked!!!!!{}'.format(market_id,
                                                                                                    follow_up))

    def _iterate_not_recovered_ending_condition(self, resp_json: dict, market_config: MarketConfig) -> bool:
        return 'entries' not in resp_json['trades'] or \
               len(resp_json['trades']['entries']) == 0 or \
               resp_json['trades']['last_timestamp'] is None

    def _update_market_config(self, resp_json: dict, market_config: MarketConfig) -> None:
        pass

    def _get_first_timestamp_from_response(self, resp_json: dict) -> int:
        return int(resp_json['trades']['entries'][0][0])

    def _get_last_timestamp_from_response(self, resp_json: dict) -> Union[int, None]:
        if resp_json['trades']['last_timestamp'] is not None:
            return int(resp_json['trades']['last_timestamp'])
        else:
            return None

    def _persist_new_entries(self, resp_json: dict, market_config: MarketConfig) -> None:
        buda_list = BudaMarketTradeList()
        buda_list.append_raw(resp_json['trades']['entries'])

        self.persistor.persist(buda_list)

