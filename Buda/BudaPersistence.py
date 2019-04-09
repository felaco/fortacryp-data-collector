import pandas as pd
import os

from Buda.BudaIntegrationConfig import BudaMarketTradeList


class BudaPersistenceBase:
    market: str = None

    def persist(self, market_list: BudaMarketTradeList):
        pass

    def persist_secondary(self, market_list: BudaMarketTradeList):
        pass

    def set_market(self, market):
        self.market = market


class BudaCsvPersistence(BudaPersistenceBase):
    # path should point to a folder
    def __init__(self, path: str):
        self.path = path

        if os.path.isfile(path):
            self.path = os.path.dirname(path)

        if not self.path.endswith('/'):
            self.path = self.path + '/'

    def persist(self, market_list: BudaMarketTradeList):
        if self.market is None:
            raise AttributeError('market attribute of the instance should not be None')

        path = os.path.join(self.path, self.market + '.csv')
        if not market_list.is_resampled():
            market_list.resample_ohlcv()

        if os.path.isfile(path):
            stored = pd.read_csv(path, sep=',', encoding='utf-8', parse_dates=True, index_col='date')

            stored_market_list = BudaMarketTradeList()
            stored_market_list.trade_list = stored

            market_list.merge(stored_market_list)

        market_list.trade_list.to_csv(path, encoding='utf-8')
